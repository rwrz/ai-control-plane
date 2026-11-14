//go:build !linux && !darwin

package netx

import "errors"

// setReusePort is unsupported here. Returning an error rather than silently
// ignoring the request keeps a deploy that RELIES on port sharing from starting
// in a configuration where restarts would drop connections.
func setReusePort(uintptr) error {
	return errors.New("SO_REUSEPORT is not supported on this platform")
}
