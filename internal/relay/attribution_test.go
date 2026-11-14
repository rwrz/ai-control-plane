package relay

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const testPrefix = "/v1/relay/deepgram"

// newAttributionRequest builds a request the way net/http would deliver it.
func newAttributionRequest(target string, headers http.Header) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	for name, values := range headers {
		for _, v := range values {
			r.Header.Add(name, v)
		}
	}
	return r
}

// The three channels are interchangeable. A caller uses whichever its SDK
// allows, and the relay must read the same values out of any of them.
func TestAttributionReadsEveryChannel(t *testing.T) {
	segment := EncodeAttributionSegment(url.Values{
		queryRelayKey:   {"path-key"},
		queryUser:       {"path-user"},
		queryModule:     {"path-module"},
		queryEntityType: {"channel"},
		queryEntityID:   {"ch-1"},
	})

	tests := []struct {
		name    string
		target  string
		headers http.Header
	}{
		{
			name:   "header",
			target: testPrefix + "/v1/listen",
			headers: http.Header{
				"X-Relay-Key":         {"path-key"},
				"X-Usage-User":        {"path-user"},
				"X-Usage-Module":      {"path-module"},
				"X-Usage-Entity-Type": {"channel"},
				"X-Usage-Entity-Id":   {"ch-1"},
			},
		},
		{
			name: "query",
			target: testPrefix + "/v1/listen?relay_key=path-key" +
				"&usage_user=path-user&usage_module=path-module" +
				"&usage_entity_type=channel&usage_entity_id=ch-1",
		},
		{
			name:   "path segment",
			target: testPrefix + "/" + segment + "/v1/listen",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAttributionRequest(tt.target, tt.headers)
			a := readAttribution(r, testPrefix)

			if a.credential != "path-key" {
				t.Errorf("credential = %q, want path-key", a.credential)
			}
			if a.meta.User != "path-user" {
				t.Errorf("user = %q, want path-user", a.meta.User)
			}
			if a.meta.Module != "path-module" {
				t.Errorf("module = %q, want path-module", a.meta.Module)
			}
			if a.meta.EntityType != "channel" || a.meta.EntityID != "ch-1" {
				t.Errorf("entity = (%q, %q), want (channel, ch-1)", a.meta.EntityType, a.meta.EntityID)
			}
			if r.URL.Path != testPrefix+"/v1/listen" {
				t.Errorf("path = %q, want the segment removed", r.URL.Path)
			}
		})
	}
}

// Whatever channel carried them, none of it may survive into the forwarded
// request — this is the half that, if it were a separate step, would eventually
// be forgotten on some path.
func TestAttributionStripsEveryChannel(t *testing.T) {
	segment := EncodeAttributionSegment(url.Values{queryRelayKey: {"path-key"}})
	r := newAttributionRequest(
		testPrefix+"/"+segment+"/v1/listen?relay_key=q-key&usage_user=q-user&model=nova-3",
		http.Header{
			"X-Relay-Key":     {"h-key"},
			"X-Usage-Tenant":  {"h-tenant"},
			"X-Usage-Unknown": {"h-unknown"},
		})

	readAttribution(r, testPrefix)

	if got := r.Header.Get("X-Relay-Key"); got != "" {
		t.Errorf("X-Relay-Key survived: %q", got)
	}
	for name := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), "X-Usage-") {
			t.Errorf("attribution header survived: %s", name)
		}
	}
	if q := r.URL.Query(); q.Has(queryRelayKey) || q.Has(queryUser) {
		t.Errorf("attribution query survived: %q", r.URL.RawQuery)
	}
	// The provider's own parameters must be untouched.
	if got := r.URL.Query().Get("model"); got != "nova-3" {
		t.Errorf("provider query lost: model = %q", got)
	}
	if strings.Contains(r.URL.String(), segment) {
		t.Errorf("attribution segment survived in %q", r.URL.String())
	}
}

// Header beats query beats path. A client that can set a header is the one
// least likely to have it rewritten in transit, so it wins.
func TestAttributionChannelPrecedence(t *testing.T) {
	segment := EncodeAttributionSegment(url.Values{
		queryUser:    {"path-user"},
		queryFeature: {"path-feature"},
	})
	r := newAttributionRequest(
		testPrefix+"/"+segment+"/v1/listen?usage_user=query-user",
		http.Header{"X-Usage-User": {"header-user"}})

	a := readAttribution(r, testPrefix)
	if a.meta.User != "header-user" {
		t.Errorf("user = %q, want the header to win", a.meta.User)
	}
	// A field only the path carried still comes through.
	if a.meta.Feature != "path-feature" {
		t.Errorf("feature = %q, want path-feature", a.meta.Feature)
	}

	r = newAttributionRequest(testPrefix+"/"+segment+"/v1/listen?usage_user=query-user", nil)
	if a := readAttribution(r, testPrefix); a.meta.User != "query-user" {
		t.Errorf("user = %q, want the query to beat the path", a.meta.User)
	}
}

// A segment we cannot decode is left alone rather than guessed at. The upstream
// then rejects the path, which is a far clearer failure than relaying with
// silently empty attribution.
func TestAttributionIgnoresUndecodableSegment(t *testing.T) {
	r := newAttributionRequest(testPrefix+"/~not!valid!base64/v1/listen", nil)
	a := readAttribution(r, testPrefix)

	if a.credential != "" || a.meta.User != "" {
		t.Errorf("undecodable segment produced attribution: %+v", a)
	}
	if r.URL.Path != testPrefix+"/~not!valid!base64/v1/listen" {
		t.Errorf("path was altered: %q", r.URL.Path)
	}
}

// The path segment is a CROSS-LANGUAGE contract: the voice agent (Python) encodes it,
// this decodes it. Nothing in either repo's test suite would otherwise notice
// the two drifting apart — the relay would just see an ordinary path, forward
// it, and meter every voice session with no attribution at all.
//
// These are real segments produced by the voice agent's AIRelay._attribution_segment,
// pasted verbatim. All but the first predate the entity pair and send
// usage_session, so they also pin the legacy alias for sidecars not yet upgraded. The awkward one pins the encoding details that differ
// between languages: Python's urlencode writes a space as "+" and escapes "/"
// as %2F, and Go's url.ParseQuery must read both back.
func TestAttributionDecodesSegmentsProducedByPythonClients(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		want    map[string]string
	}{
		{
			name:    "entity session",
			segment: "~dXNhZ2VfdGVuYW50PXZvaWNlX2FnZW50JnVzYWdlX21vZHVsZT1jb2FjaCZ1c2FnZV9mZWF0dXJlPWxpdmVfdHJhbnNjcmlwdGlvbiZyZWxheV9rZXk9c2VjcmV0LWtleSZ1c2FnZV91c2VyPXVzZXItNDImdXNhZ2VfZW50aXR5X3R5cGU9c2Vzc2lvbiZ1c2FnZV9lbnRpdHlfaWQ9c2Vzcy03",
			want: map[string]string{
				"tenant": "voice_agent", "module": "coach", "feature": "live_transcription",
				"user": "user-42", "entity_type": "session", "entity_id": "sess-7", "credential": "secret-key",
			},
		},
		{
			name:    "legacy session",
			segment: "~dXNhZ2VfdGVuYW50PXZvaWNlX2FnZW50JnVzYWdlX21vZHVsZT1jb2FjaCZ1c2FnZV9mZWF0dXJlPWxpdmVfdHJhbnNjcmlwdGlvbiZyZWxheV9rZXk9c2VjcmV0LWtleSZ1c2FnZV91c2VyPXVzZXItNDImdXNhZ2Vfc2Vzc2lvbj1zZXNzLTc",
			want: map[string]string{
				"tenant": "voice_agent", "module": "coach", "feature": "live_transcription",
				"user": "user-42", "entity_type": "session", "entity_id": "sess-7", "credential": "secret-key",
			},
		},
		{
			name:    "values needing escapes",
			segment: "~dXNhZ2VfdGVuYW50PXZvaWNlX2FnZW50JnVzYWdlX21vZHVsZT1jb2FjaCZ1c2FnZV9mZWF0dXJlPWxpdmVfdHJhbnNjcmlwdGlvbiZyZWxheV9rZXk9ayZ1c2FnZV91c2VyPXVzZXIrNDIrJUMzJUE3JUMzJUEzbyZ1c2FnZV9zZXNzaW9uPXMlMkY3",
			want: map[string]string{
				"tenant": "voice_agent", "module": "coach", "feature": "live_transcription",
				"user": "user 42 ção", "entity_type": "session", "entity_id": "s/7", "credential": "k",
			},
		},
		{
			// Scribe is the one relayed path that declares its own model, and it
			// has to: the response never names it and the request is multipart
			// audio this relay will not buffer to find out. An undeclared row is
			// REJECTED centrally as "model is required" rather than recorded
			// unpriced, so this segment is all that stands between the voice agent and a
			// transcription bill that silently disappears.
			name:    "scribe declares its model",
			segment: "~dXNhZ2VfdGVuYW50PXZvaWNlX2FnZW50JnVzYWdlX21vZHVsZT1jb2FjaCZ1c2FnZV9mZWF0dXJlPWxpdmVfdHJhbnNjcmlwdGlvbiZyZWxheV9rZXk9c2VjcmV0LWtleSZ1c2FnZV91c2VyPXVzZXItNDImdXNhZ2Vfc2Vzc2lvbj1zZXNzLTcmdXNhZ2VfbW9kZWw9c2NyaWJlX3Yy",
			want: map[string]string{
				"tenant": "voice_agent", "module": "coach", "feature": "live_transcription",
				"user": "user-42", "entity_type": "session", "entity_id": "sess-7", "credential": "secret-key",
				"model": "scribe_v2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAttributionRequest(testPrefix+"/"+tt.segment+"/v1/listen", nil)
			a := readAttribution(r, testPrefix)

			got := map[string]string{
				"tenant": a.meta.Tenant, "module": a.meta.Module, "feature": a.meta.Feature,
				"user": a.meta.User, "entity_type": a.meta.EntityType, "entity_id": a.meta.EntityID, "credential": a.credential,
				"model": a.meta.ModelHint,
			}
			for key, want := range tt.want {
				if got[key] != want {
					t.Errorf("%s = %q, want %q", key, got[key], want)
				}
			}
			if r.URL.Path != testPrefix+"/v1/listen" {
				t.Errorf("path = %q, want the segment stripped", r.URL.Path)
			}
		})
	}
}

// A path that merely looks like ours deeper down is a provider path, not an
// attribution segment. Accepting one anywhere would let a caller hide a claim
// inside a real upstream resource path.
func TestAttributionSegmentOnlyRecognizedAtTheFront(t *testing.T) {
	segment := EncodeAttributionSegment(url.Values{queryUser: {"sneaky"}})
	r := newAttributionRequest(testPrefix+"/v1/listen/"+segment, nil)

	if a := readAttribution(r, testPrefix); a.meta.User != "" {
		t.Errorf("user = %q, want a deep segment to be ignored", a.meta.User)
	}
	if !strings.HasSuffix(r.URL.Path, segment) {
		t.Errorf("path = %q, want the provider path left intact", r.URL.Path)
	}
}

// The legacy session header is an alias for consumers not yet upgraded, never
// a second grouping: a caller mid-migration may send both, and the explicit
// entity is the one it meant.
func TestResolveEntity(t *testing.T) {
	tests := []struct {
		name                       string
		entityType, entityID, sess string
		wantType, wantID           string
	}{
		{"explicit entity", "channel", "ch-1", "", "channel", "ch-1"},
		{"legacy session only", "", "", "sess-7", "session", "sess-7"},
		{"explicit wins over legacy", "channel", "ch-1", "sess-7", "channel", "ch-1"},
		{"neither", "", "", "", "", ""},
		{"type without id is dropped", "channel", "", "", "", ""},
		{"id without type is dropped", "", "ch-1", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotID := ResolveEntity(tt.entityType, tt.entityID, tt.sess)
			if gotType != tt.wantType || gotID != tt.wantID {
				t.Errorf("got (%q, %q), want (%q, %q)", gotType, gotID, tt.wantType, tt.wantID)
			}
		})
	}
}
