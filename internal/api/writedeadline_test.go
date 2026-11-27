package api

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/exitip"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/require"
)

// deadlineTestServer builds an API test server whose Config carries the given
// WriteTimeout. httptest.NewServer leaves Config.WriteTimeout at zero and
// starts serving immediately, so setting it after the fact races the serve
// goroutine's readRequest (which reads Config.WriteTimeout). Build an
// unstarted server, set the timeout, THEN start — the Config write happens
// before any connection is served. ReadTimeout is left at zero so these tests
// isolate the WRITE deadline; production's ReadTimeout=30s is what bounds a
// dribbled request body (WriteTimeout does not interrupt a blocked body read).
// The optional wire callbacks run on the Server before it is wrapped and
// started, so a test can attach fake runners without racing the serve
// goroutine (never assign hs.Config.Handler after Start).
func deadlineTestServer(t *testing.T, writeTimeout time.Duration, wire ...func(*Server)) (*httptest.Server, *config.Store, *metrics.Store) {
	t.Helper()
	cfg := config.NewStore()
	ms, err := metrics.Open(t.TempDir() + "/api.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	srv := New(cfg, ms)
	for _, w := range wire {
		w(srv)
	}
	hs := httptest.NewUnstartedServer(srv.Auth(""))
	hs.Config.WriteTimeout = writeTimeout
	hs.Start()
	t.Cleanup(func() { hs.Close() })
	return hs, cfg, ms
}

// throttledReadBody reads a response on an already-open connection slowly but
// steadily (16KB chunks with an 8ms pause ≈ 2MB/s), so a multi-MB body takes
// seconds — well past a small flat WriteTimeout. It returns the complete
// decoded body, failing if the server severed the response mid-body. A steady
// reader, not a stalled one: this is the legitimate large-read case
// writeListJSON's extended deadline exists to serve.
func throttledReadBody(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	var got strings.Builder
	buf := make([]byte, 16<<10)
	for {
		n, rerr := conn.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if rerr != nil {
			break
		}
		time.Sleep(8 * time.Millisecond)
	}
	// Parse with http.ReadResponse so chunked transfer-encoding is decoded
	// (httptest servers may chunk the body; a raw header/body split would
	// leave chunk-size lines in the payload and fail the JSON unmarshal).
	resp, err := http.ReadResponse(bufio.NewReader(strings.NewReader(got.String())), nil)
	require.NoError(t, err, "response truncated before headers completed?")
	payload, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err, "list response truncated mid-body by the flat WriteTimeout (writeListJSON not extending the deadline?)")
	return payload
}

// throttledGetBody issues a GET on a fresh raw connection and returns the
// complete body read by throttledReadBody.
func throttledGetBody(t *testing.T, hs *httptest.Server, path string) []byte {
	t.Helper()
	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	_, err = io.WriteString(conn, "GET "+path+" HTTP/1.1\r\nHost: "+hs.Listener.Addr().String()+"\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	return throttledReadBody(t, conn)
}

// throttledPostBody issues a POST with the given JSON body on a fresh raw
// connection and returns the complete response body read by
// throttledReadBody — the trigger endpoints' 200-complete branch answers the
// same history-class payloads as the list GETs.
func throttledPostBody(t *testing.T, hs *httptest.Server, path, body string) []byte {
	t.Helper()
	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
	head := "POST " + path + " HTTP/1.1\r\nHost: " + hs.Listener.Addr().String() + "\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: close\r\n\r\n"
	_, err = io.WriteString(conn, head+body)
	require.NoError(t, err)
	return throttledReadBody(t, conn)
}

// TestListWriteDeadlineSurvivesSlowReader pins writeListJSON on the probe
// history GET: a large response read slowly (but steadily) must NOT be severed
// by the server's flat WriteTimeout. Pre-fix the list handlers used writeJSON,
// so the flat deadline (armed at header-read end) cut the response mid-body —
// truncated JSON — even though the client was reading the whole time. Post-fix
// writeListJSON pushes the write deadline out, so a steady reader gets the
// complete payload. Non-vacuous: reverting writeListJSON to writeJSON makes
// this fail with "unexpected EOF".
func TestListWriteDeadlineSurvivesSlowReader(t *testing.T) {
	hs, _, ms := deadlineTestServer(t, 200*time.Millisecond)

	// Seed enough rows that the JSON body is several MB — large enough that
	// the server's write buffer cannot hold it all, so the response write
	// blocks on the throttled reader and the flat WriteTimeout fires mid-body
	// pre-fix. Each row carries an ~8KB error string; 600 rows ≈ 5MB.
	// RecordHTTPProbe is synchronous, so the rows are durable before the GET.
	base := time.Now().Add(-time.Hour)
	blob := strings.Repeat("x", 8<<10)
	for i := 0; i < 600; i++ {
		require.NoError(t, ms.RecordHTTPProbe(core.HTTPProbe{
			ID:        "p" + strconv.Itoa(i),
			Route:     "outbound:a",
			Target:    "https://example.com",
			Status:    502,
			Error:     blob,
			StartedAt: base.Add(time.Duration(i) * time.Millisecond),
		}))
	}

	payload := throttledGetBody(t, hs, "/api/probes/results?n=600")
	var rows []core.HTTPProbe
	require.NoError(t, json.Unmarshal(payload, &rows), "list response was not complete JSON")
	require.Len(t, rows, 600, "expected all 600 rows; got a truncated body")
}

// TestJobAndConfigListWriteDeadlineSurvivesSlowReader pins the extension of
// writeListJSON to the full-probe job snapshot, the job list, and every
// config collection GET (inbounds/outbounds/pools/chains/providers). Each is
// bounded only by n × row-size (a job snapshot carries every level's success
// paths; a provider fleet grows the outbound collection without a row cap),
// so each must survive a steady slow reader under a small flat WriteTimeout.
// Non-vacuous per site: each subtest seeds a multi-MB payload into ITS OWN
// collection/endpoint, so reverting that one call site to writeJSON
// truncates exactly that subtest's body here (verified per-site with
// single-call-site revert overlays).
func TestJobAndConfigListWriteDeadlineSurvivesSlowReader(t *testing.T) {
	hs, cfg, ms := deadlineTestServer(t, 200*time.Millisecond)
	blob := strings.Repeat("x", 8<<10)

	// A durable finished job whose single level carries 400 success paths,
	// each with an ~8KB hop string ≈ 3.2MB snapshot. RecordFullProbeJob writes
	// terminal jobs synchronously, so it is durable before the GETs.
	paths := make([]core.FullProbePath, 400)
	for i := range paths {
		paths[i] = core.FullProbePath{Hops: []string{blob}, Latency: time.Millisecond, Status: 200}
	}
	require.NoError(t, ms.RecordFullProbeJob(&core.FullProbeJob{
		ID:     "bigjob",
		State:  core.FullProbeDone,
		Levels: []core.FullProbeLevel{{Depth: 1, SuccessPaths: paths}},
	}))

	// Each config collection gets 400 entries carrying an ~8KB blob ≈ 3.2MB.
	// The setters store specs verbatim (no validation), so the blob rides the
	// field each collection naturally grows: a user's credential, pool
	// membership ids, chain layers, an outbound's / a provider's plugin
	// config. The store is API-only here (no engine watching), so nothing
	// tries to bind these inbounds or build these routes.
	for i := 0; i < 400; i++ {
		id := strconv.Itoa(i)
		cfg.SetInbound(&config.Inbound{
			ID:       "in" + id,
			Protocol: "socks5",
			Listen:   "127.0.0.1:0",
			Users:    []config.User{{Username: blob, Password: "p"}},
		})
		cfg.SetOutbound(&config.OutboundSpec{
			ID:       "o" + id,
			Protocol: "socks5",
			Config:   map[string]any{"blob": blob},
		})
		cfg.SetPool(&config.Pool{ID: "pool" + id, OutboundIDs: []string{blob}})
		cfg.SetChain(&config.ChainSpec{ID: "chain" + id, Layers: []string{blob}})
		cfg.SetProvider(&config.ProviderSpec{
			ID:       "prov" + id,
			Provider: "test",
			Config:   map[string]any{"blob": blob},
		})
	}

	t.Run("job snapshot", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/probes/full/bigjob")
		var j core.FullProbeJob
		require.NoError(t, json.Unmarshal(payload, &j), "job snapshot was not complete JSON")
		require.Equal(t, "bigjob", j.ID)
		require.Len(t, j.Levels, 1)
		require.Len(t, j.Levels[0].SuccessPaths, 400)
	})
	t.Run("job list", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/probes/full")
		var jobs []*core.FullProbeJob
		require.NoError(t, json.Unmarshal(payload, &jobs), "job list was not complete JSON")
		require.Len(t, jobs, 1)
		require.Equal(t, "bigjob", jobs[0].ID)
	})
	t.Run("inbound collection", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/inbounds")
		var ins []*config.Inbound
		require.NoError(t, json.Unmarshal(payload, &ins), "inbound collection was not complete JSON")
		require.Len(t, ins, 400)
	})
	t.Run("outbound collection", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/outbounds")
		var obs []*config.OutboundSpec
		require.NoError(t, json.Unmarshal(payload, &obs), "outbound collection was not complete JSON")
		require.Len(t, obs, 400)
	})
	t.Run("pool collection", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/pools")
		var ps []*config.Pool
		require.NoError(t, json.Unmarshal(payload, &ps), "pool collection was not complete JSON")
		require.Len(t, ps, 400)
	})
	t.Run("chain collection", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/chains")
		var cs []*config.ChainSpec
		require.NoError(t, json.Unmarshal(payload, &cs), "chain collection was not complete JSON")
		require.Len(t, cs, 400)
	})
	t.Run("provider collection", func(t *testing.T) {
		payload := throttledGetBody(t, hs, "/api/providers")
		var pv []*config.ProviderSpec
		require.NoError(t, json.Unmarshal(payload, &pv), "provider collection was not complete JSON")
		require.Len(t, pv, 400)
	})
}

// TestTriggerCompleteWriteDeadlineSurvivesSlowReader pins writeListJSON on
// the trigger endpoints' 200-complete branches: when a round finishes inside
// the wait window, its response IS the round's records — the same
// history-class payloads the results GETs serve (probe rows carry up to 1MiB
// of response body each; exit-check rounds are uncapped by fleet size) — and
// the wait window has already spent part of the flat write budget. A steady
// slow reader must get the whole body. Non-vacuous: reverting either
// 200-complete branch to writeJSON truncates that subtest's body here.
func TestTriggerCompleteWriteDeadlineSurvivesSlowReader(t *testing.T) {
	blob := strings.Repeat("x", 8<<10)

	t.Run("probe round complete", func(t *testing.T) {
		rows := make([]core.HTTPProbe, 400)
		for i := range rows {
			rows[i] = core.HTTPProbe{
				ID:      "p" + strconv.Itoa(i),
				Route:   "outbound:a",
				Target:  "https://example.com",
				Status:  200,
				Body:    blob,
				Success: true,
			}
		}
		hs, _, _ := deadlineTestServer(t, 200*time.Millisecond, func(srv *Server) {
			srv.SetProbeRunner(&fakeProber{res: rows})
		})

		payload := throttledPostBody(t, hs, "/api/probes/run", `{"targets":["https://example.com"]}`)
		var got []core.HTTPProbe
		require.NoError(t, json.Unmarshal(payload, &got), "probe-round response was not complete JSON")
		require.Len(t, got, 400)
	})
	t.Run("exit round complete", func(t *testing.T) {
		checks := make([]core.ExitCheck, 400)
		for i := range checks {
			checks[i] = core.ExitCheck{
				ID:         "c" + strconv.Itoa(i),
				OutboundID: "o" + strconv.Itoa(i),
				Site:       "ifconfig.me",
				Error:      blob,
			}
		}
		hs, _, _ := deadlineTestServer(t, 200*time.Millisecond, func(srv *Server) {
			srv.SetExitRunner(&fakeRunner{res: exitip.RunResult{Checks: checks}})
		})

		payload := throttledPostBody(t, hs, "/api/exit-checks/run", `{}`)
		var got exitRunResponse
		require.NoError(t, json.Unmarshal(payload, &got), "exit-round response was not complete JSON")
		require.Equal(t, "complete", got.Status)
		require.Len(t, got.Checks, 400)
	})
}

// TestConfigWriteResponseSeveredByFlatWriteTimeout pins that the deadline
// extension is scoped to large reads: a config write keeps the flat
// WriteTimeout. WriteTimeout is purely a WRITE deadline — it does not interrupt
// a handler blocked reading the request body (that is ReadTimeout's job, and it
// is zero here). So we send headers + a partial body, let the flat write
// deadline expire, THEN complete the body: readJSON finishes, the handler
// commits, but its 201 response write lands after the expired deadline and the
// server severs the connection without delivering it. A list-style 10min
// extension leaked into the config branch would deliver the 201 instead.
//
// Honest semantics: the write DOES commit (WriteTimeout bounds response
// delivery, not the commit; production's ReadTimeout=30s bounds the body read).
// We assert both — response severed, commit landed — so the test cannot pass
// vacuously from an incomplete body that never commits.
func TestConfigWriteResponseSeveredByFlatWriteTimeout(t *testing.T) {
	hs, _, _ := deadlineTestServer(t, 250*time.Millisecond)

	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	body := `{"id":"flatbound","layers":["direct"]}`
	head := "POST /api/chains HTTP/1.1\r\nHost: " + hs.Listener.Addr().String() + "\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	// Headers + only the first few body bytes: readJSON blocks waiting for the
	// rest. The flat write deadline (armed at header-read end) expires while the
	// handler is still in the body read.
	_, err = io.WriteString(conn, head+body[:5])
	require.NoError(t, err)
	time.Sleep(700 * time.Millisecond)
	// Complete the body. readJSON finishes, the handler commits and writes 201 —
	// but the write deadline has expired, so the response is severed.
	_, err = io.WriteString(conn, body[5:])
	require.NoError(t, err)

	_, err = http.ReadResponse(bufio.NewReader(conn), nil)
	require.Error(t, err, "config-write response should be severed by the flat WriteTimeout (a list-style extension leaked into the config branch?)")

	// The commit still landed: WriteTimeout bounds response delivery, not the
	// write itself.
	client := hs.Client()
	gresp, err := client.Get(hs.URL + "/api/chains/flatbound")
	require.NoError(t, err)
	io.Copy(io.Discard, gresp.Body)
	gresp.Body.Close()
	require.Equal(t, http.StatusOK, gresp.StatusCode, "the config write commits even though its response was severed")
}

// TestWriteMuReleasedWhenConfigResponseStalls is the strongest pin of the
// config-write flat bound: a client that sends a COMPLETE config write but
// never reads the (large) response must not pin writeMu. The handler holds
// writeMu across validate+commit+response-write; with tiny socket buffers the
// ~800KB response write blocks, the flat WriteTimeout fires, the write fails,
// the handler returns and releases writeMu. An unrelated config write then
// completes promptly. A list-style 10min extension leaked into the config
// branch would hold writeMu for 10 minutes and stall the second write.
func TestWriteMuReleasedWhenConfigResponseStalls(t *testing.T) {
	cfg := config.NewStore()
	ms, err := metrics.Open(t.TempDir() + "/api.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	hs := httptest.NewUnstartedServer(New(cfg, ms).Auth(""))
	hs.Config.WriteTimeout = 250 * time.Millisecond
	// Shrink the server's per-connection write buffer so even a sub-MB response
	// cannot be absorbed by the socket and the response write genuinely blocks
	// on the non-reading client.
	hs.Config.ConnState = func(c net.Conn, st http.ConnState) {
		if st == http.StateNew {
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetWriteBuffer(2048)
			}
		}
	}
	hs.Start()
	t.Cleanup(func() { hs.Close() })

	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(2048)
	}
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	// ~800KB chain echo (8000 layers × 100 chars), under the 1MiB body cap.
	layers := make([]string, 8000)
	for i := range layers {
		layers[i] = strings.Repeat("x", 100)
	}
	body, err := json.Marshal(config.ChainSpec{ID: "stall", Layers: layers})
	require.NoError(t, err)
	head := "POST /api/chains HTTP/1.1\r\nHost: " + hs.Listener.Addr().String() + "\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	_, err = io.WriteString(conn, head)
	require.NoError(t, err)
	_, err = conn.Write(body)
	require.NoError(t, err)
	// The handler reads the whole body, takes writeMu, commits, then blocks
	// writing the ~800KB response to the non-reading client. The flat 250ms
	// deadline fires, the write fails, writeMu is released.

	time.Sleep(600 * time.Millisecond) // past the flat deadline; a 10min leak still holds writeMu

	client := hs.Client()
	client.Timeout = 3 * time.Second
	start := time.Now()
	resp, err := client.Post(hs.URL+"/api/chains", "application/json", strings.NewReader(`{"id":"after","layers":["direct"]}`))
	require.NoError(t, err, "writeMu not released: the flat WriteTimeout did not sever the stalled config response (extension leaked into a config write?)")
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Less(t, time.Since(start), 2*time.Second, "an unrelated config write should complete promptly once the flat deadline releases writeMu")

	// The stalled connection was severed by the flat write deadline: drain the
	// few KB the server buffered before the deadline fired, then the read fails
	// (EOF/reset). The response must be truncated, not delivered in full.
	var received int
	var rerr error
	rbuf := make([]byte, 4096)
	for {
		n, e := conn.Read(rbuf)
		received += n
		if e != nil {
			rerr = e
			break
		}
	}
	require.Error(t, rerr, "stalled config connection should be severed by the flat WriteTimeout")
	require.Less(t, received, 100<<10, "the ~800KB response should be truncated by the flat deadline, not delivered in full")
}

// TestWriteDeadlineReArmedPerKeepAliveRequest pins that writeListJSON's
// extension does not leak across requests on a keep-alive connection: net/http
// re-arms the flat WriteTimeout per request, so a config write following a list
// GET on the SAME connection is bounded by the flat deadline, not the list
// GET's 10min extension. Without the re-arm the second request would inherit
// the extended deadline and deliver a response the flat bound should sever.
func TestWriteDeadlineReArmedPerKeepAliveRequest(t *testing.T) {
	hs, _, _ := deadlineTestServer(t, 250*time.Millisecond)
	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))
	br := bufio.NewReader(conn)
	addr := hs.Listener.Addr().String()

	// Request 1: a list GET — writeListJSON extends the write deadline to 10min.
	_, err = io.WriteString(conn, "GET /api/probes/results?n=1 HTTP/1.1\r\nHost: "+addr+"\r\n\r\n")
	require.NoError(t, err)
	resp1, err := http.ReadResponse(br, nil)
	require.NoError(t, err)
	io.Copy(io.Discard, resp1.Body)
	resp1.Body.Close()
	require.Equal(t, http.StatusOK, resp1.StatusCode)

	// Request 2 on the SAME keep-alive connection: a config write. Send headers
	// + partial body, let the re-armed flat deadline expire, then complete it.
	body := `{"id":"rearm","layers":["direct"]}`
	head := "POST /api/chains HTTP/1.1\r\nHost: " + addr + "\r\nContent-Type: application/json\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	_, err = io.WriteString(conn, head+body[:5])
	require.NoError(t, err)
	time.Sleep(700 * time.Millisecond)
	_, err = io.WriteString(conn, body[5:])
	require.NoError(t, err)

	_, err = http.ReadResponse(br, nil)
	require.Error(t, err, "the list GET's 10min extension leaked across keep-alive requests (flat WriteTimeout not re-armed?)")
}
