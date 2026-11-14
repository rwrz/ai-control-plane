package relay

import (
	"bytes"
	"io"
	"net/http"
)

// Making a proxied request retryable, and the two production incidents that
// established why it is not optional.
//
// httputil.ReverseProxy derives its outbound request from the inbound SERVER
// request, and a server request's GetBody is ALWAYS nil: its body is a live
// network stream, not a replayable buffer. Clone copies that nil through. Go's
// transport can only retry a request whose body rewinds, so every proxied POST
// is one broken connection away from a hard failure — no matter which HTTP
// version is in use:
//
//	HTTP/2    a graceful-shutdown GOAWAY arrives mid-request and the transport
//	          cannot replay it:
//	          "cannot retry err [... GOAWAY] after Request.Body was written;
//	           define Request.GetBody to avoid this error"
//	HTTP/1.1  a pooled keep-alive connection the server has already closed is
//	          handed to the next request: EOF on the read, "use of closed
//	          network connection" on the concurrent write.
//
// Deepgram exposed both. Its pre-recorded endpoint holds the connection open
// for tens of seconds while it fetches and transcribes a file, so there is a
// wide window for either event to land in. Forcing HTTP/1.1 to dodge the first
// walked straight into the second, roughly a hundred times more often, because
// it removed a trigger without repairing the cause.
//
// Setting GetBody repairs the cause. Go then retries these connection-level
// failures itself, transparently, on both protocols — which is exactly what the
// GOAWAY error text was asking for all along.
//
// Retry safety: the transport only replays when the failure says the request
// was not processed — nothing was written, the server closed an idle
// connection, or the response could not be read from a REUSED connection. Those
// are the standard conditions under which HTTP treats a retry as safe, and the
// calls this protects (transcribe the file at this URL) are idempotent in any
// case. The alternative is not "no duplicate work", it is a hard failure.
const maxRewindBytes = 256 << 10 // 256 KiB

// makeRewindable buffers a small request body so the transport can replay it,
// and leaves a large one streaming untouched.
//
// The cap is the whole design. Every control-plane payload we relay is JSON in
// the kilobytes — Deepgram's pre-recorded body is a few hundred bytes naming a
// file URL — so this buys retryability for effectively all traffic at a cost
// bounded per in-flight request. Above the cap the body streams exactly as
// before and stays non-replayable: ElevenLabs Scribe uploads multipart audio on
// a relayed path, and buffering megabytes to make it retryable would trade a
// rare failure for a memory profile that scales with concurrency.
//
// Bodyless requests are left alone: the transport already treats them as
// replayable.
func makeRewindable(r *http.Request) {
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	// A declared length over the cap needs no read at all to reject.
	if r.ContentLength > maxRewindBytes {
		return
	}

	// One byte past the cap, so an undeclared oversized body is detectable
	// rather than silently truncated.
	buf, err := io.ReadAll(io.LimitReader(r.Body, maxRewindBytes+1))
	if err != nil || len(buf) > maxRewindBytes {
		// Put back what was read and stream the rest. Not being able to rewind
		// is the status quo, never a reason to fail the call.
		r.Body = prefixedBody{
			Reader: io.MultiReader(bytes.NewReader(buf), r.Body),
			Closer: r.Body,
		}
		return
	}

	// The original body is fully consumed; close it rather than leaking it, and
	// serve the buffer from here on.
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(buf))
	r.ContentLength = int64(len(buf))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(buf)), nil
	}
}
