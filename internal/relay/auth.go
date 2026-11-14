package relay

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
)

// Authentication for the relay.
//
// The relay holds every provider's real API key. Exposed without auth it is an
// open proxy to paid AI accounts, so this must be configured before the service
// is reachable from outside the cluster.
//
// The credential travels in OUR OWN channel, never in the provider's credential
// slot. Providers disagree about where a key lives — Authorization: Bearer for
// OpenAI, x-goog-api-key for Gemini, Authorization: Token for Deepgram,
// xi-api-key for ElevenLabs — and a future one may use a query parameter or no
// header at all. Borrowing that slot would mean new extraction logic per
// provider and a mechanism that cannot work for a provider we have not met yet.
//
// It is accepted in a header OR a query parameter. The query parameter is not
// redundant: WebSocket clients frequently cannot set custom headers on the
// handshake but can always add a query parameter, and Phase 2 audio is a
// WebSocket path. Both are stripped before the request reaches the provider.
const (
	authHeader   = "X-Relay-Key"
	authQueryKey = "relay_key"
)

// Enforcement modes for the keyring.
//
// Turning authentication ON is the dangerous half of this feature, and not for
// the reason it first appears. The relay is IN the request path: a consumer
// that does not present a key does not merely go unmetered, its AI calls fail
// outright. So the moment RELAY_AUTH_KEYS is set, every consumer that has not
// yet been redeployed with a key loses the feature it was relaying — which is
// exactly what happened to the backend's dictionary in staging on 2026-07-22.
//
// Shadow mode is the way through: keys are checked and every unauthenticated
// caller is named in the logs, but nothing is rejected. Deploy the relay in
// shadow, roll keys out to consumers, wait for the warnings to stop, then flip
// to enforce with evidence rather than hope.
const (
	// AuthEnforce rejects any caller without a valid credential. The default:
	// a relay that fails open by accident is an open proxy to paid accounts.
	AuthEnforce = "enforce"
	// AuthShadow checks credentials and logs failures but serves the request,
	// attributing it the way an unauthenticated relay would.
	AuthShadow = "shadow"
)

// ParseAuthMode validates the configured mode. An empty value means enforce, so
// the safe behaviour is what you get by saying nothing.
func ParseAuthMode(s string) (string, error) {
	switch mode := strings.ToLower(strings.TrimSpace(s)); mode {
	case "", AuthEnforce:
		return AuthEnforce, nil
	case AuthShadow:
		return AuthShadow, nil
	default:
		return "", fmt.Errorf("auth mode: want %q or %q, got %q",
			AuthEnforce, AuthShadow, s)
	}
}

// Keyring maps a caller credential to the tenant it is allowed to bill.
//
// Binding a key to a tenant is what stops one consumer attributing its spend to
// another: a leaked the voice agent key cannot book cost against the Go backend, whatever it
// puts in X-Usage-Tenant. It is also the seam Phase 2 virtual keys replace —
// those are minted per unit of work and additionally carry user/feature/entity,
// but they resolve to the same decision made here.
type Keyring struct {
	// keys is credential -> tenant. Small by construction (one per consumer),
	// so a linear constant-time scan costs nothing and avoids leaking which
	// prefix matched through map lookup timing.
	keys map[string]string
}

// ParseKeyring reads "key1:tenant1,key2:tenant2". An empty spec yields a
// disabled keyring, which leaves the relay open — correct for local development
// and for the current in-cluster-only deployment, and loudly logged at boot.
func ParseKeyring(spec string) (*Keyring, error) {
	kr := &Keyring{keys: map[string]string{}}
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return kr, nil
	}

	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, tenant, found := strings.Cut(entry, ":")
		key, tenant = strings.TrimSpace(key), strings.TrimSpace(tenant)
		if !found || key == "" || tenant == "" {
			// Naming the malformed entry would put a credential in the logs.
			return nil, fmt.Errorf("auth keys: entry %d is not \"key:tenant\"",
				len(kr.keys)+1)
		}
		if _, dup := kr.keys[key]; dup {
			return nil, fmt.Errorf("auth keys: duplicate key for tenant %q", tenant)
		}
		kr.keys[key] = tenant
	}
	return kr, nil
}

// Enabled reports whether any credential is configured. A disabled keyring
// authenticates nothing and every request is allowed through.
func (k *Keyring) Enabled() bool { return k != nil && len(k.keys) > 0 }

// Tenants returns the configured tenant names, for logging what is enabled
// without ever printing a credential.
func (k *Keyring) Tenants() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(k.keys))
	for _, tenant := range k.keys {
		if _, ok := seen[tenant]; ok {
			continue
		}
		seen[tenant] = struct{}{}
		out = append(out, tenant)
	}
	return out
}

// Lookup resolves a credential to its tenant.
//
// Every configured key is compared even after a match, and with a constant-time
// comparison, so neither the number of comparisons nor their duration reveals
// how much of a guessed credential was correct.
func (k *Keyring) Lookup(candidate string) (string, bool) {
	var (
		tenant string
		found  bool
	)
	if candidate == "" {
		return "", false
	}
	for key, t := range k.keys {
		if subtle.ConstantTimeCompare([]byte(key), []byte(candidate)) == 1 {
			tenant, found = t, true
		}
	}
	return tenant, found
}

// CredentialFrom pulls the relay credential out of the request and removes it,
// so it can never be forwarded upstream. The header wins over the query
// parameter when both are present.
//
// Exported because the usage-ingest endpoint authenticates the same consumers
// with the same credential: a consumer presents one key whether it is relaying
// a call or reporting one it metered itself.
func CredentialFrom(r *http.Request) string {
	credential := r.Header.Get(authHeader)
	r.Header.Del(authHeader)

	if q := r.URL.Query(); q.Has(authQueryKey) {
		if credential == "" {
			credential = q.Get(authQueryKey)
		}
		q.Del(authQueryKey)
		r.URL.RawQuery = q.Encode()
	}
	return credential
}
