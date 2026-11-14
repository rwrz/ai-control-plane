package relay

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// WebSocket relaying.
//
// Audio is where the money is: a coaching session streams minutes of speech
// through Deepgram and back through a TTS provider, and none of it is an HTTP
// request. Relaying it means proxying the socket.
//
// The relay does NOT terminate the protocol. It completes the handshake against
// the upstream on the caller's behalf, then copies frames through byte for byte
// while reading them in passing (see wsframe.go). Terminating would mean
// re-originating subprotocol negotiation, close codes, ping/pong timing and
// fragmentation correctly for every provider forever; observing costs one frame
// parser and cannot change what either side receives.

// wsHandshakeTimeout bounds the upstream connect + handshake. The conversation
// that follows is unbounded — a voice session legitimately lasts minutes — so
// this covers only getting it started.
const wsHandshakeTimeout = 20 * time.Second

// isWebSocketUpgrade reports whether this request is a WebSocket handshake.
// Both header values are case-insensitive, and Connection is a comma-separated
// list that may carry more than just "Upgrade".
func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, token := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
			return true
		}
	}
	return false
}

// hopByHopWSHeaders are connection-scoped and describe the hop the caller made
// to us, not the one we make upstream. They are rebuilt from our own dial.
var hopByHopWSHeaders = []string{
	"Connection",
	"Upgrade",
	"Sec-Websocket-Key",
	"Sec-Websocket-Version",
	"Sec-Websocket-Extensions",
	"Sec-Websocket-Accept",
}

// serveWebSocket relays one WebSocket conversation and records what it cost.
//
// meta is already resolved and stripped from the request by the caller, exactly
// as on the HTTP path, so the two share one attribution contract.
func (p *Provider) serveWebSocket(w http.ResponseWriter, r *http.Request, meta Meta) {
	// Counted from the very start, before the upstream dial: a socket being
	// established is as much a conversation in progress as one already
	// streaming, and shutting down through a half-open handshake produces the
	// same broken session.
	if p.sockets != nil {
		p.sockets.enter()
		defer p.sockets.leave()
	}

	upstreamURL := *r.URL
	upstreamURL.Scheme = p.target.Scheme
	upstreamURL.Host = p.target.Host
	upstreamURL.Path = strings.TrimPrefix(r.URL.Path, p.prefix)

	meter := p.streamMeter(&upstreamURL)
	if meter == nil {
		// Relay it anyway. An unmetered socket is a gap in the numbers; a
		// refused one is a broken product feature, and the caller cannot tell
		// the relay lacks a meter for it.
		p.log.Warn("no stream meter for path; socket is unmetered",
			"path", upstreamURL.Path, "module", meta.Module)
	}

	upstream, upstreamReader, resp, err := p.dialUpstreamRetrying(r, &upstreamURL)
	if err != nil {
		p.log.Error("websocket upstream dial failed", "err", err, "path", upstreamURL.Path)
		http.Error(w, "relay: upstream websocket unavailable", http.StatusBadGateway)
		return
	}
	defer upstream.Close()

	// A non-101 is the upstream refusing the handshake — bad key, bad model,
	// malformed query. Pass it through as the ordinary HTTP response it is, so
	// the caller sees the provider's own explanation rather than a dead socket.
	if resp.StatusCode != http.StatusSwitchingProtocols {
		// Say WHY, not just that. the voice agent's traffic is almost entirely
		// WebSocket, so a bad key, an unknown model or a malformed query all
		// arrive here — and a status with no reason is the same dead end that
		// made a two-and-a-half-hour hunt out of an OpenAI 400 on the HTTP
		// path. The provider's explanation is in the body; the prefix is
		// bounded because the body still has to reach the caller intact and
		// some providers echo the request back.
		detail := p.errorDetail(resp)
		p.log.Error("upstream rejected websocket handshake",
			"status", resp.StatusCode, "path", upstreamURL.Path,
			"tenant", meta.Tenant, "module", meta.Module, "feature", meta.Feature,
			"detail", detail)
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		_ = resp.Body.Close()
		return
	}

	client, clientBuf, err := hijack(w)
	if err != nil {
		p.log.Error("cannot hijack client connection", "err", err)
		http.Error(w, "relay: websocket unsupported", http.StatusInternalServerError)
		return
	}
	defer client.Close()

	// Hand the upstream's 101 back to the caller, but re-derive the accept
	// token for OUR hop.
	//
	// Sec-WebSocket-Accept is a hash of the CLIENT's Sec-WebSocket-Key, and the
	// relay dialled upstream with a fresh key of its own — so the token the
	// provider computed answers a challenge the client never issued, and a
	// conforming client rejects the handshake outright ("invalid challenge
	// response"). Everything else in the response is the provider's and is
	// passed through untouched, including the subprotocol it selected.
	if key := r.Header.Get("Sec-WebSocket-Key"); key != "" {
		resp.Header.Set("Sec-WebSocket-Accept", wsAcceptToken(key))
	}
	if err := resp.Write(clientBuf); err != nil {
		p.log.Error("cannot forward handshake response", "err", err)
		return
	}
	if err := clientBuf.Flush(); err != nil {
		p.log.Error("cannot flush handshake response", "err", err)
		return
	}

	p.pumpBoth(client, clientBuf, upstream, upstreamReader, meter)

	if meter != nil {
		p.record(meta, meter.Usage())
	}
}

// pumpBoth copies frames in both directions until either side closes, then
// returns. Closing both connections on the way out unblocks whichever pump is
// still waiting on a read.
// upstreamReader MUST be the reader the handshake was read through, never a
// fresh one over the same connection. A provider that sends its first frames
// immediately after the 101 — Deepgram does, and so does anything with a
// greeting — has those bytes pulled into the handshake reader's buffer along
// with the response headers. Reading from a new bufio.Reader silently starts
// after them, so the conversation works and the meter never sees the messages
// that carry the bill.
func (p *Provider) pumpBoth(client net.Conn, clientBuf *bufio.ReadWriter, upstream net.Conn, upstreamReader *bufio.Reader, meter extract.StreamMeter) {
	observe := func(dir extract.StreamDirection) func(extract.StreamMessage) {
		if meter == nil {
			return func(extract.StreamMessage) {}
		}
		return func(msg extract.StreamMessage) { meter.Observe(dir, msg) }
	}
	buffers := func(dir extract.StreamDirection) bool {
		return meter != nil && meter.Buffers(dir)
	}

	upstreamBuf := bufio.NewWriter(upstream)
	done := make(chan struct{}, 2)

	// degraded is logged at ERROR, not warn: the call survived, but this
	// connection is now unmetered and its cost row will be short or missing.
	// It also means the frame parser lost the thread against real provider
	// traffic, which is the signal that matters most here.
	degraded := func(direction string) func(error) {
		return func(err error) {
			p.log.Error("websocket frame parsing failed; relaying blind, metering stopped",
				"err", err, "direction", direction, "provider", p.name)
		}
	}

	// Caller → provider. Reads through the hijacked reader, which may already
	// hold bytes the client sent immediately after the handshake.
	go func() {
		defer func() { done <- struct{}{} }()
		err := pump(pumpConfig{
			dst:       upstreamBuf,
			src:       clientBuf,
			buffer:    buffers(extract.FromClient),
			observe:   observe(extract.FromClient),
			onDegrade: degraded("client->provider"),
		})
		if err != nil && !isExpectedClose(err) {
			p.log.Debug("client stream ended", "err", err)
		}
	}()

	// Provider → caller.
	go func() {
		defer func() { done <- struct{}{} }()
		err := pump(pumpConfig{
			dst:       clientBuf,
			src:       upstreamReader,
			buffer:    buffers(extract.FromUpstream),
			observe:   observe(extract.FromUpstream),
			onDegrade: degraded("provider->client"),
		})
		if err != nil && !isExpectedClose(err) {
			p.log.Debug("upstream stream ended", "err", err)
		}
	}()

	// One direction ending ends the conversation: a WebSocket close is
	// bidirectional, and leaving the other pump blocked would leak a goroutine
	// and a connection for as long as the peer stays silent.
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
}

// wsDialRetryPause is the wait before the single handshake retry. Long enough
// that an immediate re-dial does not simply hit the same half-second of trouble,
// short enough to disappear inside session setup.
const wsDialRetryPause = 200 * time.Millisecond

// dialUpstreamRetrying makes one extra attempt at the handshake.
//
// A voice session begins with this dial, so a transient blip here is not a
// dropped packet the user never notices — it is a coaching session that failed
// to start, which is what they see. Unlike the HTTP path, none of this can be
// left to the transport: dialUpstream speaks to the socket directly, so there
// is no http.Transport underneath doing its own retry.
//
// Retrying is safe by construction. Only a CONNECTION-level failure gets here —
// dial, handshake write, or handshake read — so nothing has been established
// and no audio has flowed. A provider that answers with a non-101 (bad key,
// unknown model, malformed query) returns a response and NO error, and is
// deliberately not retried: that is a considered answer, and repeating it would
// only double the latency before the caller learns what is wrong.
//
// One retry, not a loop with backoff. If the provider is genuinely down, the
// honest outcome is a fast failure the caller can act on, not a session that
// takes half a minute to admit it.
func (p *Provider) dialUpstreamRetrying(r *http.Request, upstreamURL *url.URL) (net.Conn, *bufio.Reader, *http.Response, error) {
	conn, reader, resp, err := p.dialUpstream(r, upstreamURL)
	if err == nil {
		return conn, reader, resp, nil
	}

	// A caller that has already given up gets no second attempt: the socket it
	// was for is gone, and dialing again only spends a provider connection.
	ctx := r.Context()
	if ctx.Err() != nil {
		return nil, nil, nil, err
	}

	p.log.Warn("websocket upstream dial failed; retrying once",
		"err", err, "path", upstreamURL.Path)

	select {
	case <-ctx.Done():
		return nil, nil, nil, err
	case <-time.After(wsDialRetryPause):
	}

	conn, reader, resp, retryErr := p.dialUpstream(r, upstreamURL)
	if retryErr != nil {
		// Report the retry's error, but say that a first attempt also failed —
		// otherwise the log reads like a single unlucky dial.
		return nil, nil, nil, fmt.Errorf("after retry: %w (first attempt: %v)", retryErr, err)
	}
	p.log.Info("websocket upstream dial succeeded on retry", "path", upstreamURL.Path)
	return conn, reader, resp, nil
}

// dialUpstream connects to the provider and performs the WebSocket handshake on
// the caller's behalf, injecting the real credential.
//
// It returns the buffered reader the handshake was read through as well as the
// connection: it may already hold the provider's first frames, and dropping it
// drops them. See pumpBoth.
func (p *Provider) dialUpstream(r *http.Request, upstreamURL *url.URL) (net.Conn, *bufio.Reader, *http.Response, error) {
	req := r.Clone(r.Context())
	req.URL = upstreamURL
	req.Host = upstreamURL.Host
	req.RequestURI = ""
	req.Header.Del("Authorization") // drop the caller's credential
	if p.injectAuth != nil {
		p.injectAuth(req)
	}

	// Rebuild the handshake headers for OUR hop rather than forwarding the
	// caller's. Sec-WebSocket-Key in particular is per-connection.
	upgradeHeaders := http.Header{}
	for _, h := range hopByHopWSHeaders {
		if v := req.Header.Get(h); v != "" && !strings.EqualFold(h, "Sec-Websocket-Extensions") {
			upgradeHeaders.Set(h, v)
		}
		req.Header.Del(h)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", newWebSocketKey())
	if sub := upgradeHeaders.Get("Sec-Websocket-Protocol"); sub != "" {
		req.Header.Set("Sec-WebSocket-Protocol", sub)
	}
	// Refuse permessage-deflate. A compressed payload is unreadable to the
	// meter, and a relay that silently meters nothing is a far worse outcome
	// than a little extra bandwidth on an internal hop. This is the same trap
	// the Accept-Encoding deletion avoids on the HTTP path — and it hid there
	// for exactly as long as nobody tested with a real client.
	req.Header.Del("Sec-WebSocket-Extensions")

	conn, err := dialNet(upstreamURL, wsHandshakeTimeout)
	if err != nil {
		return nil, nil, nil, err
	}

	// The handshake is bounded; the conversation after it is not, so the
	// deadline is cleared once the upstream has answered.
	_ = conn.SetDeadline(time.Now().Add(wsHandshakeTimeout))
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("write handshake: %w", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("read handshake: %w", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, reader, resp, nil
}

// dialNet opens a TCP (and, for wss/https, TLS) connection to the upstream.
func dialNet(u *url.URL, timeout time.Duration) (net.Conn, error) {
	secure := u.Scheme == "https" || u.Scheme == "wss"

	host := u.Host
	if u.Port() == "" {
		if secure {
			host = net.JoinHostPort(u.Hostname(), "443")
		} else {
			host = net.JoinHostPort(u.Hostname(), "80")
		}
	}

	dialer := &net.Dialer{Timeout: timeout}
	if !secure {
		return dialer.Dial("tcp", host)
	}
	return tls.DialWithDialer(dialer, "tcp", host, &tls.Config{ServerName: u.Hostname()})
}

// hijack takes the raw client connection away from net/http so frames can be
// copied directly.
func hijack(w http.ResponseWriter) (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return nil, nil, err
	}
	// A hijacked connection inherits the server's timeouts, which would kill a
	// long-lived voice session mid-sentence.
	_ = conn.SetDeadline(time.Time{})
	return conn, buf, nil
}

// wsGUID is the constant RFC 6455 §4.2.2 appends to a client's key before
// hashing it into the accept token.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsAcceptToken computes the Sec-WebSocket-Accept answer to a client's
// Sec-WebSocket-Key. It proves the server understood the upgrade rather than
// blindly echoing a 101, and clients do check it.
func wsAcceptToken(clientKey string) string {
	sum := sha1.Sum([]byte(clientKey + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// newWebSocketKey returns the 16 random bytes, base64-encoded, that RFC 6455
// requires on the handshake. It is not a secret and proves nothing about
// identity — it only lets a client verify the server understood the upgrade —
// but it must be fresh per connection.
func newWebSocketKey() string {
	var key [16]byte
	if _, err := rand.Read(key[:]); err != nil {
		// crypto/rand does not fail in practice, and there is nothing useful to
		// do here: the handshake needs a value, and a fixed one still produces a
		// working connection because we are the party that would check it.
		return base64.StdEncoding.EncodeToString(key[:])
	}
	return base64.StdEncoding.EncodeToString(key[:])
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		for _, v := range values {
			dst.Add(key, v)
		}
	}
}

// isExpectedClose reports whether an error is the ordinary end of a
// conversation rather than a fault worth reporting. A closed socket is how
// every WebSocket ends.
func isExpectedClose(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		strings.Contains(err.Error(), "use of closed network connection")
}
