package api

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestWriteMuNotHeldDuringBodyRead pins the parse-before-lock ordering: a
// client that sends headers promptly but dribbles a small JSON body must not
// hold writeMu while the server waits for the rest of the bytes. Pre-fix
// every body-reading write branch locked writeMu BEFORE readJSON, so the
// stalled Decode blocked every other config write (and full-probe apply)
// indefinitely — MaxBytesReader bounds size, not time, and the admin server
// had no ReadTimeout. The unrelated write below then times out; post-fix it
// completes while the slow body is still pending.
func TestWriteMuNotHeldDuringBodyRead(t *testing.T) {
	hs, _, _ := newAPIServer(t, "")

	// Raw connection so we control exactly when body bytes arrive.
	conn, err := net.Dial("tcp", hs.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	body := `{"id":"slow","protocol":"http","listen":"127.0.0.1:0"}`
	head := fmt.Sprintf("POST /api/inbounds HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n",
		hs.Listener.Addr().String(), len(body))
	// Headers + only the first few body bytes: readJSON's Decode now blocks
	// waiting for the rest, well under maxBodyBytes so nothing trips the
	// size cap.
	if _, err := io.WriteString(conn, head+body[:5]); err != nil {
		t.Fatalf("write partial request: %v", err)
	}
	// Give the server goroutine time to reach the body read (and, pre-fix,
	// to take writeMu before it).
	time.Sleep(500 * time.Millisecond)

	// An unrelated config write must complete quickly while the slow body
	// is still pending. A 2s client timeout turns the pre-fix stall into a
	// hard failure instead of a hung test.
	client := hs.Client()
	client.Timeout = 2 * time.Second
	start := time.Now()
	resp, err := client.Post(hs.URL+"/api/chains", "application/json",
		strings.NewReader(`{"id":"fast","layers":["direct"]}`))
	if err != nil {
		t.Fatalf("unrelated write stalled behind the slow body reader (writeMu held across readJSON?): %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unrelated chain create: want 201, got %d", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("unrelated write took %v; the slow body must not delay it", elapsed)
	}

	// Finish the slow body: the stalled request must also complete cleanly
	// (201, or 400 if the parse rejects it — either proves the lock was
	// acquired only after the body read and released normally).
	if _, err := io.WriteString(conn, body[5:]); err != nil {
		t.Fatalf("write rest of body: %v", err)
	}
	slowResp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read stalled request's response: %v", err)
	}
	io.Copy(io.Discard, slowResp.Body)
	slowResp.Body.Close()
	if slowResp.StatusCode != http.StatusCreated && slowResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("stalled request: want 201 or 400, got %d", slowResp.StatusCode)
	}
}
