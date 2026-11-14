package relay

import (
	"context"
	"testing"
	"time"
)

// Drain must not return while a socket is open. This is the property the whole
// deploy story rests on: net/http's own Shutdown returns immediately here,
// because hijacked connections are invisible to it.
func TestDrainWaitsForOpenSockets(t *testing.T) {
	a := &activeSockets{}
	a.enter()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if a.wait(ctx) {
		t.Fatal("drain completed while a socket was still open")
	}
	if got := a.count(); got != 1 {
		t.Errorf("count = %d, want 1", got)
	}

	a.leave()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if !a.wait(ctx2) {
		t.Error("drain did not complete after the socket closed")
	}
	if got := a.count(); got != 0 {
		t.Errorf("count = %d after close, want 0", got)
	}
}

// A socket closing DURING the drain must release it, rather than the drain
// having sampled the count once and committed to waiting out the full grace.
func TestDrainCompletesWhenTheLastSocketClosesMidWait(t *testing.T) {
	a := &activeSockets{}
	a.enter()
	a.enter()

	go func() {
		time.Sleep(50 * time.Millisecond)
		a.leave()
		a.leave()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if !a.wait(ctx) {
		t.Fatal("drain timed out although both sockets closed")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("drain took %v; it waited out the grace instead of noticing the close", elapsed)
	}
}

// Nothing open drains instantly — the ordinary case, where a deploy should not
// pause at all.
func TestDrainReturnsImmediatelyWhenIdle(t *testing.T) {
	r := &Relay{sockets: &activeSockets{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	if !r.Drain(ctx) {
		t.Fatal("idle drain reported a timeout")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("idle drain took %v, want ~0", elapsed)
	}
	if got := r.ActiveWebSockets(); got != 0 {
		t.Errorf("ActiveWebSockets = %d, want 0", got)
	}
}
