// Package netx builds the service's listener.
package netx

import (
	"context"
	"net"
	"syscall"
)

// Listen opens a TCP listener on addr.
//
// With reusePort set it enables SO_REUSEPORT, which lets a SECOND process bind
// the same address while the first is still running. That is what makes a
// zero-downtime restart possible on a single machine with no load balancer in
// front — the shape of the voice agent sidecar, where the consumer connects
// straight to 127.0.0.1 and has nothing to fail over to.
//
// The sequence it enables:
//
//  1. the new container starts and binds the same port; the kernel begins
//     handing it new connections
//  2. the old container is signalled: it closes its listener, so every new
//     connection now goes to the new one, and drains its live sockets
//  3. the old container exits
//
// At no point is the port unbound, so the consumer never sees a refused
// connection. Without SO_REUSEPORT there is a window between the old process
// releasing the port and the new one taking it, and on a voice path that window
// is a failed session.
//
// Off by default: two processes silently sharing a port is a debugging trap
// when it is not deliberate — a stale container keeps serving half the traffic
// and looks exactly like a broken deploy.
func Listen(addr string, reusePort bool) (net.Listener, error) {
	if !reusePort {
		return net.Listen("tcp", addr)
	}

	cfg := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var sockErr error
			if err := c.Control(func(fd uintptr) {
				sockErr = setReusePort(fd)
			}); err != nil {
				return err
			}
			return sockErr
		},
	}
	return cfg.Listen(context.Background(), "tcp", addr)
}
