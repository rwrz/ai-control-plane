package relay

import (
	"context"
	"sync"
	"sync/atomic"
)

// Graceful shutdown for relayed WebSockets.
//
// This exists because of a specific hole in net/http: Server.Shutdown stops the
// listener and waits for in-flight *requests*, but it explicitly does NOT wait
// for — or even close — HIJACKED connections. Every audio socket the relay
// carries is hijacked (see wsproxy.go), so Shutdown returns almost immediately
// while a coaching session is mid-sentence, the process exits, and the socket
// dies with it.
//
// That is invisible in testing, where sockets are short, and unmissable in
// production, where a deploy cuts off every live conversation. A container
// stop-timeout cannot fix it either: the process is not waiting for anything,
// so it exits on its own long before the timeout matters.
//
// So the sockets are counted, and shutdown waits for them.

// activeSockets tracks in-flight WebSocket relays.
//
// The atomic counter is not redundant with the WaitGroup: a WaitGroup cannot be
// read, and a drain that cannot say how many connections it is still waiting
// for is one nobody can distinguish from a hang.
type activeSockets struct {
	wg sync.WaitGroup
	n  atomic.Int64
}

// enter registers a socket. Paired with leave, always via defer.
func (a *activeSockets) enter() {
	a.wg.Add(1)
	a.n.Add(1)
}

func (a *activeSockets) leave() {
	a.n.Add(-1)
	a.wg.Done()
}

// count reports how many sockets are still open.
func (a *activeSockets) count() int64 { return a.n.Load() }

// wait blocks until every socket has closed or ctx is done, reporting whether
// the drain completed.
func (a *activeSockets) wait(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

// ActiveWebSockets reports how many WebSocket conversations the relay is
// currently carrying. Exported for the drain log and for health reporting.
func (r *Relay) ActiveWebSockets() int64 {
	if r.sockets == nil {
		return 0
	}
	return r.sockets.count()
}

// Drain waits for in-flight WebSocket conversations to finish.
//
// Call it AFTER http.Server.Shutdown, which closes the listener: new sockets
// can no longer arrive, so this only ever waits on work already in progress.
// Returns false if ctx expired first, meaning sockets were still open and are
// about to be cut — the caller should say so rather than exit quietly.
func (r *Relay) Drain(ctx context.Context) bool {
	if r.sockets == nil {
		return true
	}
	return r.sockets.wait(ctx)
}
