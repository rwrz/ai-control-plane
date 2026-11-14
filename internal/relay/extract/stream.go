package extract

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Streaming (WebSocket) metering.
//
// A WebSocket conversation is one billable unit spread over minutes: a
// Deepgram live transcription bills the seconds of audio streamed, an
// ElevenLabs or Deepgram TTS socket bills the characters submitted. Neither is
// knowable from any single message, so a StreamMeter accumulates as frames pass
// and yields one Usage when the socket closes.
//
// The relay never terminates the protocol; it only reads messages on the way
// past. A meter therefore sees exactly what the provider and client sent, and
// must tolerate messages it does not recognise.

// StreamDirection says who sent a message.
type StreamDirection int

const (
	// FromClient is caller → provider: audio to transcribe, text to synthesise.
	FromClient StreamDirection = iota
	// FromUpstream is provider → caller: transcripts, synthesised audio,
	// end-of-stream metadata.
	FromUpstream
)

// StreamMessage is one complete WebSocket message observed by the relay.
type StreamMessage struct {
	// Text is the message content, present only for text messages in a
	// direction the meter asked to buffer. Nil otherwise — including for a text
	// message too large to buffer.
	Text []byte
	// Binary reports a binary message, whose content is never buffered.
	Binary bool
	// Size is the true payload length in bytes, always set. For an unbuffered
	// message it is the only thing known about it.
	Size int64
}

// StreamMeter accumulates the cost of one WebSocket conversation.
//
// Observe is called from both pump goroutines, so implementations must be safe
// for concurrent use. Usage is called once, after both have finished.
type StreamMeter interface {
	// Observe takes one complete message.
	Observe(dir StreamDirection, msg StreamMessage)
	// Buffers reports whether message CONTENT is needed in this direction.
	// False lets the relay stream those messages through without copying them,
	// while still reporting their size.
	//
	// This is not an optimisation detail: ElevenLabs returns synthesised audio
	// as base64 inside JSON text messages, so buffering upstream would mean
	// copying every second of every voice session through the relay's heap for
	// no reason at all.
	Buffers(dir StreamDirection) bool
	// Usage returns what the conversation cost, or nil if nothing billable was
	// observed.
	Usage() *Usage
}

// StreamMeterFor selects a meter from the upstream URL, or nil when the path
// carries nothing billable. Path-selected for the same reason response
// extractors are: one host bills several ways.
type StreamMeterFor func(*url.URL) StreamMeter

// DeepgramStreamMeter meters a Deepgram live transcription socket
// (wss://api.deepgram.com/v1/listen).
//
// Deepgram closes the conversation with a summary message:
//
//	{"type":"Metadata","duration":42.5,"channels":1,
//	 "model_info":{"<uuid>":{"name":"...","arch":"nova-3"}}}
//
// That duration is what Deepgram bills and is preferred whenever it arrives.
// It often does not: a voice session that ends because the user closed the app,
// the pod restarted, or the network dropped never gets a clean close, and those
// are ordinary events rather than edge cases. Two fallbacks stand behind it:
//
//  1. the furthest transcript end seen in a Results message, which tracks the
//     audio Deepgram actually processed, and
//  2. the audio bytes the client sent, converted using the encoding and sample
//     rate from the request's own query string.
//
// Both are marked as estimates. Recording an estimate is not ideal; recording
// zero for a fifteen-minute call because the socket broke is worse, and it is
// the failure mode that would go unnoticed for a whole billing cycle.
type DeepgramStreamMeter struct {
	mu sync.Mutex

	// Reported by Deepgram at close — authoritative.
	metadataSeconds float64
	haveMetadata    bool
	channels        int64
	model           string

	// Fallbacks.
	transcriptEnd float64
	audioBytes    int64

	// Audio geometry from the request, for the bytes fallback.
	bytesPerSecond float64
	queryChannels  int64
	queryModel     string
}

// NewDeepgramStreamMeter builds a meter for one live transcription socket,
// reading the audio geometry it may need from the request URL.
func NewDeepgramStreamMeter(u *url.URL) *DeepgramStreamMeter {
	q := u.Query()
	m := &DeepgramStreamMeter{
		queryChannels: 1,
		queryModel:    q.Get("model"),
	}
	if c, err := strconv.ParseInt(q.Get("channels"), 10, 64); err == nil && c > 0 {
		m.queryChannels = c
	}
	// Only linear PCM has a byte count that converts to a duration. Compressed
	// or containerised audio (opus, mp3, webm) does not, and guessing a
	// bitrate would produce a number that looks measured and is not.
	if rate, err := strconv.ParseInt(q.Get("sample_rate"), 10, 64); err == nil && rate > 0 {
		if bytesPerSample := linearPCMSampleBytes(q.Get("encoding")); bytesPerSample > 0 {
			m.bytesPerSecond = float64(rate) * float64(bytesPerSample) * float64(m.queryChannels)
		}
	}
	return m
}

// linearPCMSampleBytes returns the bytes per sample for the linear PCM
// encodings Deepgram accepts, or 0 for anything whose size does not map to a
// duration. Empty defaults to linear16: it is Deepgram's default and what the
// LiveKit plugin sends.
func linearPCMSampleBytes(encoding string) int {
	switch encoding {
	case "", "linear16":
		return 2
	case "linear32", "float32":
		return 4
	case "mulaw", "alaw":
		return 1
	default:
		return 0
	}
}

func (m *DeepgramStreamMeter) Buffers(dir StreamDirection) bool {
	// Upstream is small JSON worth reading; the client is sending raw audio.
	return dir == FromUpstream
}

func (m *DeepgramStreamMeter) Observe(dir StreamDirection, msg StreamMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if dir == FromClient {
		if msg.Binary {
			m.audioBytes += msg.Size
		}
		return
	}
	if len(msg.Text) == 0 {
		return
	}

	var r struct {
		Type      string   `json:"type"`
		Duration  *float64 `json:"duration"`
		Start     *float64 `json:"start"`
		Channels  *int64   `json:"channels"`
		ModelInfo map[string]struct {
			Name string `json:"name"`
			Arch string `json:"arch"`
		} `json:"model_info"`
	}
	if json.Unmarshal(msg.Text, &r) != nil {
		return
	}

	switch r.Type {
	case "Metadata":
		if r.Duration == nil {
			return
		}
		m.metadataSeconds, m.haveMetadata = *r.Duration, true
		m.channels = deref(r.Channels)
		for _, info := range r.ModelInfo {
			if info.Arch != "" {
				m.model = info.Arch
			} else {
				m.model = info.Name
			}
			break
		}
	case "Results":
		// start + duration is where this chunk of audio ended. Taking the
		// maximum rather than summing is deliberate: interim results repeat
		// and overlap, and summing them would bill the same audio many times.
		if r.Start != nil && r.Duration != nil {
			if end := *r.Start + *r.Duration; end > m.transcriptEnd {
				m.transcriptEnd = end
			}
		}
	}
}

func (m *DeepgramStreamMeter) Usage() *Usage {
	m.mu.Lock()
	defer m.mu.Unlock()

	model := m.model
	if model == "" {
		model = m.queryModel
	}

	seconds, estimate := m.metadataSeconds, false
	channels := m.channels
	if !m.haveMetadata {
		estimate = true
		channels = m.queryChannels
		seconds = m.transcriptEnd
		if seconds == 0 && m.bytesPerSecond > 0 {
			// bytesPerSecond already accounts for channel count, so this yields
			// wall-clock seconds like every other branch.
			seconds = float64(m.audioBytes) / m.bytesPerSecond
		}
	}
	if seconds <= 0 {
		return nil
	}
	if channels < 1 {
		channels = 1
	}

	return &Usage{
		Model:    model,
		Modality: ModalitySTT,
		// Deepgram bills per channel-second: two channels of a 60s call is 120.
		AudioSeconds:       seconds * float64(channels),
		SecondsAreEstimate: estimate,
	}
}

// TextSubmissionMeter meters a TTS socket by the text the client submits.
//
// Deepgram Speak and ElevenLabs both work this way — the client streams JSON
// messages carrying text, the provider streams audio back, and the bill is
// characters in, not audio out. They differ only in which field holds the text,
// so one meter covers both.
//
// Characters are counted in RUNES, not bytes. Providers bill per character and
// a multi-byte character is one character; counting bytes would overcharge
// every non-ASCII language, which for a language-learning product is most of
// them.
type TextSubmissionMeter struct {
	mu    sync.Mutex
	runes int64

	// fields are the JSON keys that hold billable text, in the order they
	// should be tried.
	fields []string
	model  string
}

// NewElevenLabsTTSStreamMeter meters an ElevenLabs TTS socket, whose client
// messages are {"text":"..."} — including the single-space priming message and
// the empty message that closes the stream, both of which ElevenLabs bills as
// what they contain.
func NewElevenLabsTTSStreamMeter(u *url.URL) *TextSubmissionMeter {
	return &TextSubmissionMeter{
		fields: []string{"text"},
		model:  u.Query().Get("model_id"),
	}
}

// NewDeepgramTTSStreamMeter meters a Deepgram Speak socket, whose client
// messages are {"type":"Speak","text":"..."} plus Flush/Close control messages
// that carry no text.
func NewDeepgramTTSStreamMeter(u *url.URL) *TextSubmissionMeter {
	return &TextSubmissionMeter{
		fields: []string{"text"},
		model:  u.Query().Get("model"),
	}
}

func (m *TextSubmissionMeter) Buffers(dir StreamDirection) bool {
	// The client's text is the bill. Upstream is synthesised audio — raw binary
	// for Deepgram, base64-in-JSON for ElevenLabs — and buffering it would copy
	// every second of speech through the heap to learn nothing.
	return dir == FromClient
}

func (m *TextSubmissionMeter) Observe(dir StreamDirection, msg StreamMessage) {
	if dir != FromClient || len(msg.Text) == 0 {
		return
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(msg.Text, &fields) != nil {
		return
	}
	for _, name := range m.fields {
		raw, ok := fields[name]
		if !ok {
			continue
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			continue
		}
		m.mu.Lock()
		m.runes += int64(utf8.RuneCountInString(text))
		m.mu.Unlock()
		return
	}
}

func (m *TextSubmissionMeter) Usage() *Usage {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runes == 0 {
		return nil
	}
	return &Usage{
		Model:      m.model,
		Modality:   ModalityTTS,
		Characters: m.runes,
	}
}

// InworldTTSStreamMeter meters an Inworld bidirectional synthesis socket
// (/tts/v1/voice:streamBidirectional).
//
// Inworld needs its own meter rather than the shared TextSubmissionMeter for
// two reasons, and getting either wrong means metering nothing at all while the
// audio flows perfectly:
//
//		{"create":{"voiceId":"Mark","modelId":"inworld-tts-2-flash",...},"contextId":"…"}
//		{"send_text":{"text":"Hello"},"contextId":"…"}
//		{"flush_context":{},"contextId":"…"}
//
//	 1. The billable text is nested under send_text, not at the top level.
//	 2. The MODEL arrives in the create message. Every other provider names its
//	    model in the URL; Inworld's socket URL carries nothing at all, so without
//	    reading create there is no way to price the call.
//
// This is the voice agent's primary voice, so it is also the largest single stream of
// TTS spend there is.
type InworldTTSStreamMeter struct {
	mu    sync.Mutex
	runes int64
	model string
}

// NewInworldTTSStreamMeter builds a meter for one Inworld synthesis socket.
// The URL carries no model or options, so everything comes off the wire.
func NewInworldTTSStreamMeter(*url.URL) *InworldTTSStreamMeter {
	return &InworldTTSStreamMeter{}
}

func (m *InworldTTSStreamMeter) Buffers(dir StreamDirection) bool {
	// The client's text is the bill. Upstream is synthesised audio as base64
	// inside JSON — buffering it would copy every second of speech through the
	// heap to learn nothing.
	return dir == FromClient
}

func (m *InworldTTSStreamMeter) Observe(dir StreamDirection, msg StreamMessage) {
	if dir != FromClient || len(msg.Text) == 0 {
		return
	}
	var frame struct {
		SendText *struct {
			Text string `json:"text"`
		} `json:"send_text"`
		Create *struct {
			ModelID string `json:"modelId"`
		} `json:"create"`
	}
	if json.Unmarshal(msg.Text, &frame) != nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if frame.Create != nil && frame.Create.ModelID != "" {
		m.model = frame.Create.ModelID
	}
	if frame.SendText != nil {
		m.runes += int64(utf8.RuneCountInString(frame.SendText.Text))
	}
}

func (m *InworldTTSStreamMeter) Usage() *Usage {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.runes == 0 {
		return nil
	}
	return &Usage{
		Model:      m.model,
		Modality:   ModalityTTS,
		Characters: m.runes,
	}
}

// InworldStreamMeterFor selects a meter for an Inworld socket.
//
// The path has no /v1/relay/inworld prefix: Inworld's SDK builds URLs with
// urljoin and a leading slash, which discards any prefix, so the relay serves
// its route at the root. See Relay.Handler.
func InworldStreamMeterFor(u *url.URL) StreamMeter {
	if strings.Contains(u.Path, "/tts/v1/voice") {
		return NewInworldTTSStreamMeter(u)
	}
	return nil
}

// DeepgramStreamMeterFor selects a meter for a Deepgram socket. Deepgram serves
// transcription and synthesis from the same host, billed in opposite directions
// and different units, so the path decides.
func DeepgramStreamMeterFor(u *url.URL) StreamMeter {
	switch {
	case strings.Contains(u.Path, "/listen"):
		return NewDeepgramStreamMeter(u)
	case strings.Contains(u.Path, "/speak"):
		return NewDeepgramTTSStreamMeter(u)
	}
	return nil
}

// ElevenLabsStreamMeterFor selects a meter for an ElevenLabs socket.
func ElevenLabsStreamMeterFor(u *url.URL) StreamMeter {
	if strings.Contains(u.Path, "/text-to-speech") {
		return NewElevenLabsTTSStreamMeter(u)
	}
	return nil
}
