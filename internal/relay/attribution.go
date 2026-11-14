package relay

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"

	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// Attribution and credential travel in one of three interchangeable channels.
// They exist because clients differ in what they can control, not because they
// mean different things — the same names carry the same values in all three:
//
//	header  X-Relay-Key, X-Usage-*     the default; nothing else can be spoofed
//	                                   by a URL that ends up in a log
//	query   relay_key, usage_*         for WebSocket clients, which usually
//	                                   cannot set handshake headers
//	path    /~<base64url of a query>   for WebSocket clients that cannot set a
//	                                   usable query either
//
// The path channel is not redundant with the query one. The LiveKit Deepgram
// plugin builds its URL as f"{base_url}?{urlencode(opts)}" — an unconditional
// "?" — so a base URL carrying its own query becomes "…?a=b?model=nova-3" and
// the upstream rejects it. The path survives that concatenation intact, and
// every SDK we relay (Deepgram, ElevenLabs, OpenAI, Gemini, Inworld) treats its
// base URL as an opaque prefix, so one mechanism covers all of them.
//
// This is also the seam Phase 2 virtual keys slot into: the segment becomes an
// opaque minted id the relay RESOLVES, rather than a set of values it decodes,
// without any client having to move where it puts the token.
const (
	// attributionSegmentPrefix marks the path segment as ours. Upstream paths
	// begin with an API version ("/v1/listen"), so "~" cannot collide.
	attributionSegmentPrefix = "~"

	queryRelayKey   = "relay_key"
	queryTenant     = "usage_tenant"
	queryUser       = "usage_user"
	queryFeature    = "usage_feature"
	queryModule     = "usage_module"
	queryEntityType = "usage_entity_type"
	queryEntityID   = "usage_entity_id"
	// querySession is the legacy spelling of an entity; see ResolveEntity.
	querySession = "usage_session"
	queryModel   = "usage_model"
	queryRequest = "usage_request"
)

// EncodeAttributionSegment builds the path segment a client uses when it can
// set neither headers nor a query. Exported because it is a contract shared with
// callers, and one they must not have to reimplement from the parser.
func EncodeAttributionSegment(values url.Values) string {
	return attributionSegmentPrefix +
		base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
}

// splitAttributionPath removes the attribution segment from a path and returns
// the values it carried along with the path the upstream should see.
//
// The segment is looked for only immediately after "/v1/relay/<provider>",
// where the relay puts it. Accepting it anywhere would mean a caller could hide
// one deeper in a path that also names a real upstream resource.
func splitAttributionPath(path, providerPrefix string) (url.Values, string) {
	rest, found := strings.CutPrefix(path, providerPrefix)
	if !found {
		return nil, path
	}
	rest = strings.TrimPrefix(rest, "/")
	segment, remainder, _ := strings.Cut(rest, "/")
	if !strings.HasPrefix(segment, attributionSegmentPrefix) {
		return nil, path
	}

	raw, err := base64.RawURLEncoding.DecodeString(
		strings.TrimPrefix(segment, attributionSegmentPrefix))
	if err != nil {
		// Undecodable: treat it as a normal path segment rather than guessing.
		// The upstream will reject it, which is a far clearer failure than
		// silently relaying with no attribution.
		return nil, path
	}
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil, path
	}
	return values, providerPrefix + "/" + remainder
}

// attribution is the resolved identity of one relayed request, plus the
// credential presented with it.
type attribution struct {
	meta       Meta
	credential string
}

// readAttribution pulls identity and credential out of a request through
// whichever channel carries them, and REMOVES every trace of them so nothing
// reaches the provider. Header beats query beats path: a client that can set a
// header is the one least likely to have it rewritten in transit.
//
// It mutates r deliberately. Extraction and stripping have to be the same
// operation — leaving them separate is how a credential eventually gets
// forwarded upstream by a path that forgot the second half.
func readAttribution(r *http.Request, providerPrefix string) attribution {
	pathValues, cleanPath := splitAttributionPath(r.URL.Path, providerPrefix)
	r.URL.Path = cleanPath

	query := r.URL.Query()
	queryTouched := false

	// take reads one value across all three channels and strips it as it goes.
	take := func(header, key string) string {
		value := r.Header.Get(header)
		r.Header.Del(header)

		if query.Has(key) {
			if value == "" {
				value = query.Get(key)
			}
			query.Del(key)
			queryTouched = true
		}
		if value == "" {
			value = pathValues.Get(key)
		}
		return value
	}

	a := attribution{
		credential: take(authHeader, queryRelayKey),
		meta: Meta{
			Tenant:    take("X-Usage-Tenant", queryTenant),
			User:      take("X-Usage-User", queryUser),
			Feature:   take("X-Usage-Feature", queryFeature),
			Module:    take("X-Usage-Module", queryModule),
			ModelHint: take("X-Usage-Model", queryModel),
		},
	}
	a.meta.EntityType, a.meta.EntityID = ResolveEntity(
		take("X-Usage-Entity-Type", queryEntityType),
		take("X-Usage-Entity-Id", queryEntityID),
		take("X-Usage-Session", querySession),
	)
	if raw := take("X-Usage-Request", queryRequest); raw != "" {
		if h, ok := extract.ParseHints([]byte(raw)); ok {
			a.meta.Hints = h
		}
	}

	// Any other X-Usage-* header is unknown to us but still ours, so it is
	// dropped rather than forwarded to the provider.
	for h := range r.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(h), "X-Usage-") {
			r.Header.Del(h)
		}
	}

	if queryTouched {
		r.URL.RawQuery = query.Encode()
	}
	return a
}

// LegacySessionEntityType is what a legacy session id is recorded as.
const LegacySessionEntityType = "session"

// ResolveEntity settles the (type, id) a row is grouped under.
//
// session is the pre-entity spelling, still sent by consumers deployed before
// the entity pair existed. It maps to ("session", id) so their rows keep
// grouping while they upgrade; delete this alias once none of them sends it.
// An explicit entity always wins over it, and half a pair is dropped: an
// untyped id groups with nothing, and a bare type would only pollute the
// dashboard's type list.
func ResolveEntity(entityType, entityID, session string) (string, string) {
	if entityID == "" && session != "" {
		return LegacySessionEntityType, session
	}
	if entityType == "" || entityID == "" {
		return "", ""
	}
	return entityType, entityID
}
