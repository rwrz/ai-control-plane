package relay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rwrz/ai-control-plane/internal/pricing"
	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// asyncSink collects the events the relay produced.
type asyncSink struct {
	mu     sync.Mutex
	events []Event
	got    chan struct{}
}

func newAsyncSink() *asyncSink {
	return &asyncSink{got: make(chan struct{}, 8)}
}

func (s *asyncSink) Record(_ context.Context, ev Event) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
	select {
	case s.got <- struct{}{}:
	default:
	}
}

// await waits for one event. Metering happens after the socket closes, so a
// test that reads the slice immediately races the relay.
func (s *asyncSink) await(t *testing.T) Event {
	t.Helper()
	select {
	case <-s.got:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a usage event")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events[len(s.events)-1]
}

func (s *asyncSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// wsTestPrices covers the models the socket tests use, so a priced call can be
// distinguished from an unpriced one.
func wsTestPrices() *pricing.Config {
	return &pricing.Config{Models: map[string]pricing.ModelPrice{
		"nova-3":            {PerAudioHour: 0.29},
		"eleven_turbo_v2_5": {Per1kCharacters: 0.09},
	}}
}

// wsRelay stands up a fake provider plus a relay in front of it, and returns the
// relay's base URL along with the requests the provider saw.
type wsRelay struct {
	url      string
	sink     *asyncSink
	upstream *httptest.Server

	mu       sync.Mutex
	requests []*http.Request
}

func (f *wsRelay) lastRequest(t *testing.T) *http.Request {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		t.Fatal("upstream received no request")
	}
	return f.requests[len(f.requests)-1]
}

// newWSRelay builds a relay whose single provider points at a fake upstream.
// handle runs per accepted socket and owns it.
func newWSRelay(t *testing.T, cfg ProviderConfig, handle func(*wsConn)) *wsRelay {
	t.Helper()

	f := &wsRelay{sink: newAsyncSink()}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, r.Clone(context.Background()))
		f.mu.Unlock()

		if !isWebSocketUpgrade(r) {
			http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
			return
		}
		conn, err := wsHandshake(w, r)
		if err != nil {
			return
		}
		defer conn.Close()
		handle(conn)
	}))
	t.Cleanup(f.upstream.Close)

	cfg.Target = f.upstream.URL
	if cfg.Keyring == nil {
		cfg.Keyring, _ = ParseKeyring("")
	}
	p, err := newProvider(cfg, wsTestPrices(), f.sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/v1/relay/", p)
	relaySrv := httptest.NewServer(mux)
	t.Cleanup(relaySrv.Close)

	f.url = relaySrv.URL
	return f
}

// deepgramProvider is the real Deepgram wiring, so tests exercise the same
// meter selection production does.
func deepgramProvider() ProviderConfig {
	return ProviderConfig{
		Name:        "deepgram",
		Extract:     staticExtract(extract.DeepgramExtract),
		StreamMeter: extract.DeepgramStreamMeterFor,
		InjectAuth: func(req *http.Request) {
			req.Header.Set("Authorization", "Token real-upstream-key")
		},
	}
}

// A live transcription is the highest-volume audio path we have. This is the
// end-to-end shape: masked binary audio up, JSON results down, a Metadata
// summary at close, one priced event out.
func TestWebSocketMetersDeepgramLiveTranscription(t *testing.T) {
	f := newWSRelay(t, deepgramProvider(), func(up *wsConn) {
		for {
			opcode, payload, err := up.read()
			if err != nil || opcode == opClose {
				return
			}
			if opcode == opText && strings.Contains(string(payload), "CloseStream") {
				_ = up.writeText(`{"type":"Results","start":0.0,"duration":4.5,
					"channel":{"alternatives":[{"transcript":"hello"}]}}`)
				_ = up.writeText(`{"type":"Metadata","duration":12.5,"channels":1,
					"model_info":{"c0d1":{"name":"2-general-nova","arch":"nova-3"}}}`)
				_ = up.writeClose()
				return
			}
		}
	})

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen?model=nova-3&encoding=linear16&sample_rate=16000",
		http.Header{
			"X-Usage-Tenant":  {"voice_agent"},
			"X-Usage-User":    {"user-42"},
			"X-Usage-Module":  {"coach"},
			"X-Usage-Feature": {"live_transcription"},
		})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// Stream audio the way the LiveKit plugin does: raw PCM in binary frames.
	for range 5 {
		if err := client.writeBinary(make([]byte, 640)); err != nil {
			t.Fatalf("write audio: %v", err)
		}
	}
	if err := client.writeText(`{"type":"CloseStream"}`); err != nil {
		t.Fatalf("write close stream: %v", err)
	}

	// Drain until the provider closes, so the relay finishes metering.
	for {
		opcode, _, err := client.read()
		if err != nil || opcode == opClose {
			break
		}
	}

	ev := f.sink.await(t)
	if ev.Modality != extract.ModalitySTT {
		t.Errorf("modality = %q, want %q", ev.Modality, extract.ModalitySTT)
	}
	// Deepgram's own Metadata duration wins over anything the relay could infer.
	if ev.AudioSeconds != 12.5 {
		t.Errorf("audio_seconds = %v, want 12.5", ev.AudioSeconds)
	}
	if ev.Model != "nova-3" {
		t.Errorf("model = %q, want nova-3", ev.Model)
	}
	if !ev.CostKnown || ev.CostUSD <= 0 {
		t.Errorf("cost = %v (known=%v), want a priced call", ev.CostUSD, ev.CostKnown)
	}
	if ev.User != "user-42" || ev.Tenant != "voice_agent" || ev.Feature != "live_transcription" {
		t.Errorf("attribution lost: tenant=%q user=%q feature=%q",
			ev.Tenant, ev.User, ev.Feature)
	}
}

// A voice session that ends badly — app closed, pod restarted, network dropped —
// never receives the Metadata summary. Billing zero for a real conversation is
// the failure that would go unnoticed for a whole billing cycle, so the relay
// falls back to what it saw.
func TestWebSocketEstimatesDeepgramWhenMetadataNeverArrives(t *testing.T) {
	f := newWSRelay(t, deepgramProvider(), func(up *wsConn) {
		_, _, _ = up.read()
		_ = up.writeText(`{"type":"Results","start":6.0,"duration":2.0,
			"channel":{"alternatives":[{"transcript":"partial"}]}}`)
		// No Metadata, no close handshake: just hang up.
		_ = up.Close()
	})

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen?model=nova-3", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if err := client.writeBinary(make([]byte, 320)); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	for {
		if _, _, err := client.read(); err != nil {
			break
		}
	}

	ev := f.sink.await(t)
	// start + duration of the furthest result, not the sum of overlapping ones.
	if ev.AudioSeconds != 8.0 {
		t.Errorf("audio_seconds = %v, want 8.0 from the last transcript end", ev.AudioSeconds)
	}
	if ev.Model != "nova-3" {
		t.Errorf("model = %q, want nova-3 from the query", ev.Model)
	}
}

// The relay must present its own credential and hide ours. Everything the
// caller used to identify itself has to be gone by the time the provider sees
// the request — including the attribution channels that ride in the URL.
func TestWebSocketStripsCallerIdentityAndInjectsCredential(t *testing.T) {
	cfg := deepgramProvider()
	cfg.Keyring, _ = ParseKeyring("caller-secret:voice_agent")

	f := newWSRelay(t, cfg, func(up *wsConn) {
		_ = up.writeText(`{"type":"Metadata","duration":1.0,"channels":1}`)
		_ = up.writeClose()
	})

	// The path channel: what a Deepgram client must use, since its URL builder
	// appends "?" unconditionally and would corrupt a query.
	segment := EncodeAttributionSegment(url.Values{
		queryRelayKey: {"caller-secret"},
		queryUser:     {"user-77"},
		queryModule:   {"coach"},
		queryTenant:   {"impostor"},
	})
	client, _, err := wsDial(t,
		f.url+"/v1/relay/deepgram/"+segment+"/v1/listen?model=nova-3", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	for {
		if _, _, err := client.read(); err != nil {
			break
		}
	}

	ev := f.sink.await(t)
	if ev.User != "user-77" {
		t.Errorf("user = %q, want user-77 from the path segment", ev.User)
	}
	// The authenticated key decides the tenant; the claimed one is ignored.
	if ev.Tenant != "voice_agent" {
		t.Errorf("tenant = %q, want voice_agent from the key, not the claim", ev.Tenant)
	}

	req := f.lastRequest(t)
	if got := req.Header.Get("Authorization"); got != "Token real-upstream-key" {
		t.Errorf("upstream Authorization = %q, want the relay's own credential", got)
	}
	if req.URL.Path != "/v1/listen" {
		t.Errorf("upstream path = %q, want /v1/listen with the segment removed", req.URL.Path)
	}
	if strings.Contains(req.URL.RawQuery, "caller-secret") ||
		strings.Contains(req.URL.Path, "caller-secret") {
		t.Errorf("relay credential leaked upstream in %q", req.URL.String())
	}
	for header := range req.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(header), "X-Usage-") {
			t.Errorf("attribution header %s reached the provider", header)
		}
	}
	if req.URL.Query().Get("model") != "nova-3" {
		t.Errorf("provider's own query was not preserved: %q", req.URL.RawQuery)
	}
}

// permessage-deflate would make every frame unreadable to the meter while the
// conversation still worked perfectly — metering silently records nothing. This
// is the WebSocket form of the gzip trap on the HTTP path.
func TestWebSocketRefusesCompressionOffer(t *testing.T) {
	f := newWSRelay(t, deepgramProvider(), func(up *wsConn) {
		_ = up.writeText(`{"type":"Metadata","duration":1.0,"channels":1}`)
		_ = up.writeClose()
	})

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen", http.Header{
		"Sec-WebSocket-Extensions": {"permessage-deflate; client_max_window_bits"},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	for {
		if _, _, err := client.read(); err != nil {
			break
		}
	}
	f.sink.await(t)

	if got := f.lastRequest(t).Header.Get("Sec-WebSocket-Extensions"); got != "" {
		t.Errorf("Sec-WebSocket-Extensions = %q, want it dropped so frames stay readable", got)
	}
}

// An unauthenticated socket must be refused before the relay's provider
// credential is used, exactly as on the HTTP path.
func TestWebSocketRejectsUnauthenticatedCaller(t *testing.T) {
	cfg := deepgramProvider()
	cfg.Keyring, _ = ParseKeyring("caller-secret:voice_agent")

	reached := make(chan struct{}, 1)
	f := newWSRelay(t, cfg, func(up *wsConn) {
		reached <- struct{}{}
		_ = up.writeClose()
	})

	_, resp, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
	select {
	case <-reached:
		t.Error("unauthenticated request reached the provider")
	default:
	}
}

// A provider refusing the handshake — bad model, malformed query, revoked key —
// must surface as the HTTP response it is, not as a socket that opens and dies.
func TestWebSocketPassesThroughUpstreamRejection(t *testing.T) {
	f := &wsRelay{sink: newAsyncSink()}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"err_code":"INVALID_MODEL"}`))
	}))
	t.Cleanup(f.upstream.Close)

	cfg := deepgramProvider()
	cfg.Target = f.upstream.URL
	cfg.Keyring, _ = ParseKeyring("")
	p, err := newProvider(cfg, wsTestPrices(), f.sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/relay/", p)
	relaySrv := httptest.NewServer(mux)
	t.Cleanup(relaySrv.Close)

	_, resp, err := wsDial(t, relaySrv.URL+"/v1/relay/deepgram/v1/listen", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want the provider's own 400", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "INVALID_MODEL") {
		t.Errorf("body = %q, want the provider's explanation", body)
	}
	if f.sink.count() != 0 {
		t.Error("a refused handshake was billed")
	}
}

// A message split across frames with a ping in between is legal, and half a
// JSON object parses as nothing — so the Metadata summary would be lost.
func TestWebSocketReassemblesFragmentedMessages(t *testing.T) {
	f := newWSRelay(t, deepgramProvider(), func(up *wsConn) {
		_ = up.writeFragmentedText(
			`{"type":"Metadata","duration":9.`,
			`0,"channels":1,"model_info":{"a":{"arch":"nova-3"}}}`)
		_ = up.writeClose()
	})

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	for {
		if _, _, err := client.read(); err != nil {
			break
		}
	}

	if ev := f.sink.await(t); ev.AudioSeconds != 9.0 {
		t.Errorf("audio_seconds = %v, want 9.0 reassembled across frames", ev.AudioSeconds)
	}
}

// Metering must be an observer, never a participant. If the frame parser loses
// the thread — the failure mode that would otherwise hang or drop a live call —
// the conversation has to survive, byte-for-byte, with only the cost row lost.
//
// The upstream here sends a valid message, then a header declaring a payload
// length with the high bit set — unrepresentable, and exactly the shape a
// desynced parser produces when it reads payload bytes as a header — then more
// traffic. The client must receive every byte regardless.
func TestWebSocketSurvivesUnparseableFrames(t *testing.T) {
	f := newWSRelay(t, deepgramProvider(), func(up *wsConn) {
		_ = up.writeText(`{"type":"Metadata","duration":5.0,"channels":1,
			"model_info":{"a":{"arch":"nova-3"}}}`)

		// FIN|text, length escape 127, then an 8-byte length of all 1s.
		malformed := []byte{0x81, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		if _, err := up.rw.Write(malformed); err != nil {
			return
		}
		if _, err := up.rw.WriteString("after-desync"); err != nil {
			return
		}
		_ = up.rw.Flush()
		_ = up.Close()
	})

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen?model=nova-3", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// Read raw bytes rather than frames: past the corrupt frame the relay is
	// copying blind, so what matters is that the bytes arrive unaltered.
	var received []byte
	buf := make([]byte, 4096)
	for {
		_ = client.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := client.rw.Read(buf)
		received = append(received, buf[:n]...)
		if err != nil {
			break
		}
	}

	for _, want := range []string{"Metadata", "after-desync"} {
		if !strings.Contains(string(received), want) {
			t.Errorf("client did not receive %q — the relay dropped traffic it could not parse", want)
		}
	}

	// Metering stops at the corrupt frame, but what was understood before it is
	// still recorded rather than thrown away.
	ev := f.sink.await(t)
	if ev.AudioSeconds != 5.0 {
		t.Errorf("audio_seconds = %v, want 5.0 from the message read before the desync", ev.AudioSeconds)
	}
}

// TTS over a socket bills the text sent, counted in runes. A language-learning
// product sends mostly non-ASCII text, so byte counting would overcharge nearly
// every call.
func TestWebSocketMetersElevenLabsTTSInRunes(t *testing.T) {
	cfg := ProviderConfig{
		Name:        "elevenlabs",
		Extract:     elevenLabsExtractorFor,
		StreamMeter: extract.ElevenLabsStreamMeterFor,
		InjectAuth:  func(req *http.Request) { req.Header.Set("xi-api-key", "real") },
	}
	f := newWSRelay(t, cfg, func(up *wsConn) {
		for {
			opcode, payload, err := up.read()
			if err != nil || opcode == opClose {
				return
			}
			var msg struct {
				Text string `json:"text"`
			}
			if json.Unmarshal(payload, &msg) == nil && msg.Text == "" {
				_ = up.writeClose()
				return
			}
			// Synthesised audio comes back as base64 inside JSON, which the
			// relay must stream through rather than buffer.
			_ = up.writeText(`{"audio":"UklGRiQAAABXQVZF","isFinal":false}`)
		}
	})

	client, _, err := wsDial(t,
		f.url+"/v1/relay/elevenlabs/v1/text-to-speech/voice-1/multi-stream-input?model_id=eleven_turbo_v2_5",
		http.Header{"X-Usage-Module": {"coach"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// 5 runes, 9 bytes. Billing bytes would be a 80% overcharge.
	if err := client.writeText(`{"text":"Olá çã"}`); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := client.read(); err != nil {
		t.Fatalf("read audio: %v", err)
	}
	if err := client.writeText(`{"text":""}`); err != nil {
		t.Fatalf("write terminator: %v", err)
	}
	for {
		if _, _, err := client.read(); err != nil {
			break
		}
	}

	ev := f.sink.await(t)
	if ev.Modality != extract.ModalityTTS {
		t.Errorf("modality = %q, want tts", ev.Modality)
	}
	if ev.Characters != 6 {
		t.Errorf("characters = %d, want 6 runes (\"Olá çã\"), not 9 bytes", ev.Characters)
	}
	if ev.Model != "eleven_turbo_v2_5" {
		t.Errorf("model = %q, want it read from the query", ev.Model)
	}
}

// A LIVE relayed socket must hold the drain open, and releasing it must let the
// drain finish.
//
// The unit tests around activeSockets prove the primitive; this proves it is
// actually wired into the path a coaching session takes. Getting that wrong is
// silent in exactly the worst way: the drain reports success instantly, every
// deploy looks clean, and the only symptom is conversations dying at 3am
// whenever someone ships.
func TestDrainWaitsForALiveRelayedSocket(t *testing.T) {
	release := make(chan struct{})
	cfg := deepgramProvider()
	cfg.Sockets = &activeSockets{}

	f := newWSRelay(t, cfg, func(up *wsConn) {
		// Hold the upstream side open until the test says otherwise, standing
		// in for a session still in progress.
		<-release
	})

	rel := &Relay{sockets: cfg.Sockets}

	client, _, err := wsDial(t, f.url+"/v1/relay/deepgram/v1/listen?model=nova-3", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// The socket is established asynchronously; wait for the relay to count it.
	deadline := time.Now().Add(2 * time.Second)
	for rel.ActiveWebSockets() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := rel.ActiveWebSockets(); got != 1 {
		t.Fatalf("ActiveWebSockets = %d, want 1 — the socket is not being tracked", got)
	}

	// With it open, a drain must NOT complete.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if rel.Drain(ctx) {
		t.Fatal("drain completed while a live socket was still relaying")
	}

	// Let the conversation end; the drain must then finish promptly.
	close(release)
	_ = client.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if !rel.Drain(ctx2) {
		t.Fatalf("drain never completed after the socket closed; %d still counted",
			rel.ActiveWebSockets())
	}
}

// TestWebSocketLogsWhyAHandshakeWasRejected covers the diagnostic gap that made
// a two-and-a-half-hour hunt out of an OpenAI 400 on the HTTP path. the voice agent's
// traffic is almost entirely WebSocket, so a bad key, an unknown model or a
// malformed query all arrive as a rejected handshake — and a status with no
// reason sends whoever is on call to the wrong end of the connection.
//
// The caller's copy of the body must survive being read for the log: the SDKs
// parse it.
func TestWebSocketLogsWhyAHandshakeWasRejected(t *testing.T) {
	const errBody = `{"err_code":"INVALID_AUTH","err_msg":"Invalid credentials"}`

	f := &wsRelay{sink: newAsyncSink()}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(errBody))
	}))
	t.Cleanup(f.upstream.Close)

	var logs bytes.Buffer
	cfg := deepgramProvider()
	cfg.Target = f.upstream.URL
	cfg.Keyring, _ = ParseKeyring("")
	p, err := newProvider(cfg, wsTestPrices(), f.sink, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/relay/", p)
	relaySrv := httptest.NewServer(mux)
	t.Cleanup(relaySrv.Close)

	_, resp, err := wsDial(t, relaySrv.URL+"/v1/relay/deepgram/v1/listen", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)

	if !strings.Contains(logs.String(), "INVALID_AUTH") {
		t.Errorf("the reason was not logged; log was:\n%s", logs.String())
	}
	if string(body) != errBody {
		t.Errorf("caller received a mangled body:\n got: %q\nwant: %q", body, errBody)
	}
}

// flakyWSUpstream is a raw listener that kills the first N connections before
// answering, then serves a normal handshake. httptest cannot express "refuse
// the connection", and refusing is exactly the transient failure being tested.
type flakyWSUpstream struct {
	ln        net.Listener
	mu        sync.Mutex
	attempts  int
	killFirst int
}

func newFlakyWSUpstream(t *testing.T, killFirst int) *flakyWSUpstream {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &flakyWSUpstream{ln: ln, killFirst: killFirst}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			f.attempts++
			kill := f.attempts <= f.killFirst
			f.mu.Unlock()

			if kill {
				// Close without answering: the handshake read gets EOF, which
				// is a connection-level failure and nothing more.
				_ = conn.Close()
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				_, _ = io.WriteString(c, "HTTP/1.1 101 Switching Protocols\r\n"+
					"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
					"Sec-WebSocket-Accept: "+wsAcceptToken(req.Header.Get("Sec-WebSocket-Key"))+"\r\n\r\n")
				// Hold the socket open so the caller side stays valid.
				_, _ = io.Copy(io.Discard, br)
			}(conn)
		}
	}()
	return f
}

func (f *flakyWSUpstream) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

// dialingProvider builds a provider pointed at an arbitrary host:port.
func dialingProvider(t *testing.T, addr string, logs io.Writer) *Provider {
	t.Helper()

	cfg := deepgramProvider()
	cfg.Target = "http://" + addr
	cfg.Keyring, _ = ParseKeyring("")
	p, err := newProvider(cfg, wsTestPrices(), newAsyncSink(),
		slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	return p
}

func wsUpgradeRequest(target string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", newWebSocketKey())
	return r
}

// TestWebSocketDialRetriesOnce — a voice session begins with this dial, so a
// transient blip is not a dropped packet nobody notices, it is a coaching
// session that failed to start. Nothing underneath retries for us: dialUpstream
// speaks to the socket directly, with no http.Transport beneath it.
func TestWebSocketDialRetriesOnce(t *testing.T) {
	up := newFlakyWSUpstream(t, 1) // first connection dies, second is served

	var logs bytes.Buffer
	p := dialingProvider(t, up.ln.Addr().String(), &logs)

	u, _ := url.Parse("ws://" + up.ln.Addr().String() + "/v1/listen")
	conn, _, resp, err := p.dialUpstreamRetrying(wsUpgradeRequest("/v1/relay/deepgram/v1/listen"), u)
	if err != nil {
		t.Fatalf("dial failed despite a healthy second attempt: %v", err)
	}
	defer conn.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("status = %d, want 101", resp.StatusCode)
	}
	if up.count() != 2 {
		t.Errorf("upstream saw %d connections, want exactly 2 (one failure, one retry)", up.count())
	}
	if !strings.Contains(logs.String(), "retrying once") {
		t.Error("the retry was not logged; a silent retry hides a degrading provider")
	}
}

// TestWebSocketDialGivesUpAfterOneRetry — one retry, not a loop. If the
// provider is genuinely down the honest outcome is a fast failure the caller
// can act on, not a session that takes half a minute to admit it.
func TestWebSocketDialGivesUpAfterOneRetry(t *testing.T) {
	up := newFlakyWSUpstream(t, 99) // never answers

	p := dialingProvider(t, up.ln.Addr().String(), io.Discard)

	u, _ := url.Parse("ws://" + up.ln.Addr().String() + "/v1/listen")
	_, _, _, err := p.dialUpstreamRetrying(wsUpgradeRequest("/v1/relay/deepgram/v1/listen"), u)
	if err == nil {
		t.Fatal("dial reported success against an upstream that never answers")
	}
	if !strings.Contains(err.Error(), "first attempt") {
		t.Errorf("err = %v, want it to name both attempts", err)
	}
	if up.count() != 2 {
		t.Errorf("upstream saw %d connections, want exactly 2 — no retry loop", up.count())
	}
}

// TestWebSocketDoesNotRetryAProviderRejection — a non-101 is a considered
// answer (bad key, unknown model), not a transient failure. Repeating it would
// only double the latency before the caller learns what is wrong.
func TestWebSocketDoesNotRetryAProviderRejection(t *testing.T) {
	var attempts atomic.Int32

	f := &wsRelay{sink: newAsyncSink()}
	f.upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"err_code":"INVALID_AUTH"}`))
	}))
	t.Cleanup(f.upstream.Close)

	cfg := deepgramProvider()
	cfg.Target = f.upstream.URL
	cfg.Keyring, _ = ParseKeyring("")
	p, err := newProvider(cfg, wsTestPrices(), f.sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/relay/", p)
	relaySrv := httptest.NewServer(mux)
	t.Cleanup(relaySrv.Close)

	if _, _, err := wsDial(t, relaySrv.URL+"/v1/relay/deepgram/v1/listen", nil); err != nil {
		t.Fatalf("dial: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("upstream saw %d handshakes, want 1 — a rejection must not be retried", got)
	}
}
