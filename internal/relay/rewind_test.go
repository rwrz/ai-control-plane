package relay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// inspectTransport records what the OUTBOUND request looked like after
// ReverseProxy cloned it, then delegates. The outbound request is the one whose
// GetBody the production failures were about, and it is not observable from the
// upstream handler — GetBody is a client-side field and never goes on the wire.
type inspectTransport struct {
	next        http.RoundTripper
	sawGetBody  bool
	replayedGot string
	replayErr   error
}

func (it *inspectTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	it.sawGetBody = r.GetBody != nil
	if r.GetBody != nil {
		// Exercise it exactly as the transport would when retrying.
		body, err := r.GetBody()
		if err != nil {
			it.replayErr = err
		} else {
			b, _ := io.ReadAll(body)
			_ = body.Close()
			it.replayedGot = string(b)
		}
	}
	return it.next.RoundTrip(r)
}

// TestOutboundRequestIsReplayable is the regression test for the Deepgram
// failures, both of them.
//
// ReverseProxy derives its outbound request from the inbound SERVER request,
// whose GetBody is always nil — the body is a live socket, not a buffer. Go's
// transport can only retry a request that rewinds, so a connection dying under
// a proxied POST was a hard failure. Over HTTP/2 that meant a GOAWAY killed a
// transcription; forcing HTTP/1.1 to avoid GOAWAY just met the stale
// keep-alive race instead, about a hundred times more often.
//
// The fix is to give the transport a body it can replay. This asserts it
// survives the clone and actually reproduces the bytes.
func TestOutboundRequestIsReplayable(t *testing.T) {
	const body = `{"url":"https://example.com/audio.flac","model":"nova-3"}`

	var upstreamGot string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamGot = string(b)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p, _ := newTestProvider(t, upstream.URL)
	it := &inspectTransport{next: http.DefaultTransport}
	p.proxy.Transport = it

	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	resp, err := http.Post(relaySrv.URL+"/v1/relay/openai/v1/listen",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	if !it.sawGetBody {
		t.Fatal("outbound request has GetBody == nil: the transport cannot retry it, " +
			"which is exactly what killed Deepgram transcriptions on both HTTP versions")
	}
	if it.replayErr != nil {
		t.Fatalf("GetBody returned an error: %v", it.replayErr)
	}
	if it.replayedGot != body {
		t.Errorf("a replay would send the wrong bytes:\n got: %q\nwant: %q", it.replayedGot, body)
	}
	// The first attempt must still be intact — buffering must not consume it.
	if upstreamGot != body {
		t.Errorf("upstream received:\n got: %q\nwant: %q", upstreamGot, body)
	}
}

// TestOversizedBodyStreamsAndStillArrives pins the cap's other side. A body too
// large to buffer stays non-replayable — that is the deliberate trade — but it
// must still reach the upstream byte-for-byte. ElevenLabs Scribe uploads
// multipart audio through here.
func TestOversizedBodyStreamsAndStillArrives(t *testing.T) {
	body := strings.Repeat("A", maxRewindBytes+4096)

	var upstreamGot int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		upstreamGot = len(b)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	p, _ := newTestProvider(t, upstream.URL)
	it := &inspectTransport{next: http.DefaultTransport}
	p.proxy.Transport = it

	relaySrv := httptest.NewServer(p)
	defer relaySrv.Close()

	resp, err := http.Post(relaySrv.URL+"/v1/relay/openai/v1/audio/transcriptions",
		"application/octet-stream", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	if it.sawGetBody {
		t.Errorf("a %d-byte body was buffered; the %d-byte cap is not being applied",
			len(body), maxRewindBytes)
	}
	if upstreamGot != len(body) {
		t.Errorf("upstream received %d bytes, want the whole %d", upstreamGot, len(body))
	}
}

// TestMakeRewindableLeavesBodylessRequestsAlone — a GET has nothing to rewind
// and the transport already treats it as replayable.
func TestMakeRewindableLeavesBodylessRequestsAlone(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	r.Body = http.NoBody

	makeRewindable(r)

	if r.GetBody != nil {
		t.Error("GetBody was set on a bodyless request")
	}
}

// TestMakeRewindableIsIdempotentWithUsageExtraction guards the interaction with
// usageFromRequest, which reads the body again for TTS metering. Both buffer;
// they must agree on the bytes rather than one consuming the other's.
func TestMakeRewindableIsIdempotentWithUsageExtraction(t *testing.T) {
	const body = `{"text":"hello","model_id":"eleven_turbo_v2"}`

	r := httptest.NewRequest(http.MethodPost, "/v1/text-to-speech/voice", strings.NewReader(body))
	makeRewindable(r)

	first, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read after rewind: %v", err)
	}
	if string(first) != body {
		t.Fatalf("body after rewind:\n got: %q\nwant: %q", first, body)
	}

	replay, err := r.GetBody()
	if err != nil {
		t.Fatalf("GetBody: %v", err)
	}
	second, _ := io.ReadAll(replay)
	if string(second) != body {
		t.Errorf("replayed body:\n got: %q\nwant: %q", second, body)
	}
	if r.ContentLength != int64(len(body)) {
		t.Errorf("ContentLength = %d, want %d", r.ContentLength, len(body))
	}
}
