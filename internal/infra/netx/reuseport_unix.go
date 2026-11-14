//go:build linux || darwin

package netx

import "golang.org/x/sys/unix"

// setReusePort enables SO_REUSEPORT on the socket. Linux is where this runs
// (the sidecar); darwin is where it is developed and tested.
//
// The constant comes from golang.org/x/sys/unix, not the standard syscall
// package: syscall.SO_REUSEPORT is defined on darwin and the BSDs but was never
// added for Linux, so a build that compiles on a Mac fails in the Linux image
// with "undefined: syscall.SO_REUSEPORT". x/sys defines it correctly per
// platform and is already in the module graph (pgx depends on it).
func setReusePort(fd uintptr) error {
	return unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
}
