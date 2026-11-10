// Probe-trace tests: the httptrace hook sequences net/http and net.Dialer
// actually produce. The important one is Happy Eyeballs (RFC 8305), where
// net.Dialer races two address families and the loser's ConnectDone — always
// canceled, always fired after the winner's — must not corrupt the recorded
// first-hop address or flip the failure flag the leg attribution depends on.
package router

import (
	"context"
	"errors"
	"net/http/httptrace"
	"testing"
	"time"
)

func traceHooks(t *testing.T) (*probeTrace, *httptrace.ClientTrace) {
	t.Helper()
	tr := newProbeTrace(time.Now())
	ct := httptrace.ContextClientTrace(withProbeTrace(context.Background(), tr))
	if ct == nil {
		t.Fatal("withProbeTrace installed no ClientTrace")
	}
	return tr, ct
}

func TestProbeTraceHappyEyeballsLoserDoesNotOverwriteWinner(t *testing.T) {
	tr, ct := traceHooks(t)
	ct.ConnectStart("tcp", "[2600::1]:443")
	ct.ConnectStart("tcp", "93.184.216.34:443") // fallback racer
	ct.ConnectDone("tcp", "93.184.216.34:443", nil)
	ct.ConnectDone("tcp", "[2600::1]:443", context.Canceled) // loser lands last

	p := tr.snapshot()
	if p.DialAddr != "93.184.216.34:443" {
		t.Errorf("DialAddr = %q, want the winner's address", p.DialAddr)
	}
	if !p.connectDone || p.connectFailed {
		t.Errorf("connectDone=%v connectFailed=%v, want the winner's success to stand",
			p.connectDone, p.connectFailed)
	}
}

func TestProbeTraceSuccessAfterFailureUpgrades(t *testing.T) {
	// The v6 racer fails instantly and the v4 fallback then succeeds: the
	// success is the result that matters, address included.
	tr, ct := traceHooks(t)
	ct.ConnectStart("tcp", "[2600::1]:443")
	ct.ConnectDone("tcp", "[2600::1]:443", errors.New("connect: network is unreachable"))
	ct.ConnectStart("tcp", "93.184.216.34:443")
	ct.ConnectDone("tcp", "93.184.216.34:443", nil)

	p := tr.snapshot()
	if p.DialAddr != "93.184.216.34:443" || p.connectFailed {
		t.Errorf("DialAddr=%q connectFailed=%v, want the winner", p.DialAddr, p.connectFailed)
	}
}

func TestProbeTraceLoserCancelDoesNotMaskFailure(t *testing.T) {
	tr, ct := traceHooks(t)
	ct.ConnectStart("tcp", "93.184.216.34:443")
	ct.ConnectDone("tcp", "93.184.216.34:443", errors.New("connect: connection refused"))
	ct.ConnectDone("tcp", "[2600::1]:443", context.Canceled)

	p := tr.snapshot()
	if p.DialAddr != "93.184.216.34:443" || !p.connectFailed {
		t.Errorf("DialAddr=%q connectFailed=%v, want the real failure kept", p.DialAddr, p.connectFailed)
	}
}

func TestProbeTraceProbeLevelCancelIsRecorded(t *testing.T) {
	// A cancel with nothing recorded yet is the probe's own context dying
	// mid-connect — a real result, not racer noise.
	tr, ct := traceHooks(t)
	ct.ConnectStart("tcp", "93.184.216.34:443")
	ct.ConnectDone("tcp", "93.184.216.34:443", context.Canceled)

	p := tr.snapshot()
	if !p.connectDone || !p.connectFailed || p.DialAddr != "93.184.216.34:443" {
		t.Errorf("done=%v failed=%v addr=%q, want the cancel recorded",
			p.connectDone, p.connectFailed, p.DialAddr)
	}
}

func TestProbeTraceLateStartDoesNotMoveRecordedWinner(t *testing.T) {
	tr, ct := traceHooks(t)
	ct.ConnectStart("tcp", "93.184.216.34:443")
	ct.ConnectDone("tcp", "93.184.216.34:443", nil)
	ct.ConnectStart("tcp", "[2600::1]:443") // hypothetical late racer start

	p := tr.snapshot()
	if p.DialAddr != "93.184.216.34:443" {
		t.Errorf("DialAddr = %q, want the recorded winner", p.DialAddr)
	}
}
