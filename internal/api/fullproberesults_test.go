package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedResultRows lands five attempt rows for job fp-test: two successes
// (relay depth 1, relay>work depth 2) and three failures, with IDs ordered
// r1 < r2 < ... < r5 so newest-first order and cursor semantics are
// observable. It waits for the async flush before returning.
func seedResultRows(t *testing.T, ms *metrics.Store) []core.HTTPProbe {
	t.Helper()
	mk := func(i int, route string, depth int, ok bool, reason string) core.HTTPProbe {
		p := core.HTTPProbe{
			ID:          fmt.Sprintf("%019d-%s-http://example.com", i, route),
			Route:       route,
			Target:      "http://example.com",
			Method:      "GET",
			Success:     ok,
			Depth:       depth,
			FullProbeID: "fp-test",
			Latency:     time.Duration(i) * time.Millisecond,
		}
		if ok {
			p.Status = http.StatusOK
		} else {
			p.Error = "test failure"
			p.Phase = "connect"
			p.Reason = reason
		}
		return p
	}
	rows := []core.HTTPProbe{
		mk(1, "relay", 1, true, ""),
		mk(2, "work", 1, false, "connect_refused"),
		mk(3, "relay>work", 2, true, ""),
		mk(4, "relay>dead", 2, false, "proxy_timeout"),
		mk(5, "work>relay", 2, false, "connect_refused"),
	}
	for _, p := range rows {
		for ms.RecordHTTPProbeAsync(p) != nil {
			time.Sleep(time.Millisecond)
		}
	}
	require.Eventually(t, func() bool {
		got, err := ms.FullProbeResults("fp-test", 100, "", metrics.FullProbeResultFilter{})
		require.NoError(t, err)
		return len(got) == len(rows)
	}, 5*time.Second, 20*time.Millisecond, "seed rows never flushed")
	return rows
}

// resultIDs fetches the endpoint and returns the row IDs in response order.
func resultIDs(t *testing.T, hs *httptest.Server, query string) []string {
	t.Helper()
	path := "/api/probes/full/fp-test/results"
	if query != "" {
		path += "?" + query
	}
	code, body := do(t, hs, "GET", path, "", nil)
	require.Equal(t, http.StatusOK, code, "%s: %s", path, string(body))
	var rows []core.HTTPProbe
	require.NoError(t, json.Unmarshal(body, &rows), "%s", string(body))
	var ids []string
	for _, p := range rows {
		ids = append(ids, p.ID)
	}
	return ids
}

// TestFullProbeResultsEndpoint covers the filter/pagination matrix over the
// seeded rows: newest-first order, every filter, their combination, n and the
// strictly-exclusive before cursor.
func TestFullProbeResultsEndpoint(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, _, ms := newFullProbeServer(t, fr)
	rows := seedResultRows(t, ms)

	all := []string{rows[4].ID, rows[3].ID, rows[2].ID, rows[1].ID, rows[0].ID}
	assert.Equal(t, all, resultIDs(t, hs, ""), "default order is newest first")

	cases := []struct {
		query string
		want  []string
	}{
		{"outcome=success", []string{rows[2].ID, rows[0].ID}},
		{"outcome=failed", []string{rows[4].ID, rows[3].ID, rows[1].ID}},
		{"depth=1", []string{rows[1].ID, rows[0].ID}},
		{"depth=2", []string{rows[4].ID, rows[3].ID, rows[2].ID}},
		{"reason=connect_refused", []string{rows[4].ID, rows[1].ID}},
		{"route=" + url.QueryEscape("relay>work"), []string{rows[2].ID}},
		{"hop=work", []string{rows[4].ID, rows[2].ID, rows[1].ID}},
		{"last_hop=relay", []string{rows[4].ID, rows[0].ID}},
		{"outcome=failed&hop=work", []string{rows[4].ID, rows[1].ID}},
		{"n=2", []string{rows[4].ID, rows[3].ID}},
		{"before=" + url.QueryEscape(rows[3].ID), []string{rows[2].ID, rows[1].ID, rows[0].ID}},
		{"before=" + url.QueryEscape(rows[0].ID), nil},
		// A cursor whose row was pruned still partitions the keyspace.
		{"before=" + url.QueryEscape(fmt.Sprintf("%019d-zzz", 3)), []string{rows[2].ID, rows[1].ID, rows[0].ID}},
		{"outcome=failed&n=1", []string{rows[4].ID}},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, resultIDs(t, hs, c.query), "query %q", c.query)
	}

	// The body is a bare array of full probe rows, with the failure evidence.
	code, body := do(t, hs, "GET", "/api/probes/full/fp-test/results?outcome=failed&n=1", "", nil)
	require.Equal(t, http.StatusOK, code)
	var got []core.HTTPProbe
	require.NoError(t, json.Unmarshal(body, &got))
	require.Len(t, got, 1)
	assert.Equal(t, "fp-test", got[0].FullProbeID)
	assert.Equal(t, 2, got[0].Depth)
	assert.Equal(t, "connect_refused", got[0].Reason)
	assert.NotEmpty(t, got[0].Error)

	// The shared /api/probes/results endpoint stays success-only: the seeded
	// failures never leak into it.
	code, body = do(t, hs, "GET", "/api/probes/results?n=100", "", nil)
	require.Equal(t, http.StatusOK, code)
	var shared []core.HTTPProbe
	require.NoError(t, json.Unmarshal(body, &shared))
	for _, p := range shared {
		if p.FullProbeID == "fp-test" {
			assert.True(t, p.Success, "failure leaked into the shared history: %s", p.ID)
		}
	}
}

// TestFullProbeResultsGuards covers the endpoint's status surface: 404 for an
// unknown job, 405 for non-GET, 400 for unparseable n/depth, the n bounds
// (0 → default, huge → capped), and 200 [] for a known job with no rows.
func TestFullProbeResultsGuards(t *testing.T) {
	fr := &fakeFullRunner{}
	fr.set(finishedJob())
	hs, _, ms := newFullProbeServer(t, fr)

	code, _ := do(t, hs, "GET", "/api/probes/full/fp-absent/results", "", nil)
	assert.Equal(t, http.StatusNotFound, code)
	code, _ = do(t, hs, "POST", "/api/probes/full/fp-test/results", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)
	code, _ = do(t, hs, "DELETE", "/api/probes/full/fp-test/results", "", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, code)

	for _, q := range []string{"n=abc", "n=-5", "n=99999999999999999999", "depth=abc", "depth=-1"} {
		code, body := do(t, hs, "GET", "/api/probes/full/fp-test/results?"+q, "", nil)
		assert.Equal(t, http.StatusBadRequest, code, "query %q: %s", q, string(body))
	}

	// A known job that issued no attempts (or whose rows were pruned): 200
	// with an empty array, never null and never 404.
	code, body := do(t, hs, "GET", "/api/probes/full/fp-test/results", "", nil)
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "[]\n", string(body), "empty result must be a bare empty array")

	rows := seedResultRows(t, ms)
	// n=0 falls back to the default; an oversized n is capped, not rejected.
	assert.Len(t, resultIDs(t, hs, "n=0"), len(rows))
	assert.Len(t, resultIDs(t, hs, "n=999999"), len(rows))
}

// TestFullProbeResultsDurableJob asserts the endpoint serves a job the live
// runner no longer knows (engine restart): the durable snapshot authorizes
// the 200 and the rows come from the job bucket.
func TestFullProbeResultsDurableJob(t *testing.T) {
	hs, _, ms := newFullProbeServer(t, nil) // no runner wired at all
	require.NoError(t, ms.RecordFullProbeJob(finishedJob()))
	require.Eventually(t, func() bool {
		_, ok := ms.FullProbeJob("fp-test")
		return ok
	}, 5*time.Second, 20*time.Millisecond)
	rows := seedResultRows(t, ms)

	got := resultIDs(t, hs, "outcome=success")
	assert.Equal(t, []string{rows[2].ID, rows[0].ID}, got)

	code, _ := do(t, hs, "GET", "/api/probes/full/fp-absent/results", "", nil)
	assert.Equal(t, http.StatusNotFound, code, "unknown job stays 404 without a runner")
}
