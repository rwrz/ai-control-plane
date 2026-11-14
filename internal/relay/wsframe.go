package relay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/rwrz/ai-control-plane/internal/relay/extract"
)

// RFC 6455 frame plumbing.
//
// The relay copies WebSocket frames through byte-for-byte and only READS them on
// the way past. It deliberately does not terminate the protocol: subprotocol
// negotiation, close codes, ping/pong timing and fragmentation are all the
// provider's business, and re-originating them means reimplementing every
// provider's expectations correctly forever. Parsing just enough of the frame
// header to find the payload keeps the relay transparent, which is the same
// principle the HTTP side follows.
//
// One thing IS rewritten, in the handshake rather than here: permessage-deflate
// is refused (see stripCompressionOffer). Compressed payloads are unreadable to
// the meter, and a relay that silently meters nothing is worse than one that
// costs a little bandwidth — the identical trap the Accept-Encoding deletion
// avoids on the HTTP path.

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// protocolError marks a frame the relay could not make sense of, as opposed to
// a socket that failed. The distinction decides whether the conversation dies:
// a broken socket is fatal, but a frame we cannot parse means only that OUR
// UNDERSTANDING broke — the bytes themselves are still whatever the sender
// sent, and both peers can carry on perfectly well without us reading them.
//
// See pump: this is what triggers the fall back to blind copying, which turns
// the worst failure mode (a dropped call, mid-sentence) into an acceptable one
// (a lost metering row).
type protocolError struct{ reason string }

func (e protocolError) Error() string { return "websocket: " + e.reason }

// maxBufferedMessageBytes caps how much of a single text message is held in
// memory for metering. Provider control/result JSON is kilobytes; anything
// beyond this is passed through unread rather than buffered, so a hostile or
// broken peer cannot make the relay allocate without bound.
const maxBufferedMessageBytes = 1 << 20 // 1 MiB

// copyChunkBytes is the streaming copy size for payloads that are not buffered
// (audio). Large enough that a 20ms Opus/PCM frame moves in one write.
const copyChunkBytes = 32 * 1024

// frameHeader is the parsed prefix of one WebSocket frame.
type frameHeader struct {
	fin     bool
	opcode  byte
	masked  bool
	maskKey [4]byte
	length  int64
}

// isControl reports whether this frame is close/ping/pong. Control frames may be
// interleaved between the fragments of a data message and must never be treated
// as part of it.
func (h frameHeader) isControl() bool { return h.opcode&0x8 != 0 }

// readFrameHeader parses a frame header, writing the raw bytes it consumed to
// dst so the peer receives the frame exactly as it was sent.
func readFrameHeader(dst io.Writer, src io.Reader) (frameHeader, error) {
	var h frameHeader

	var first [2]byte
	if _, err := io.ReadFull(src, first[:]); err != nil {
		return h, err
	}
	if _, err := dst.Write(first[:]); err != nil {
		return h, err
	}

	h.fin = first[0]&0x80 != 0
	h.opcode = first[0] & 0x0F
	h.masked = first[1]&0x80 != 0
	h.length = int64(first[1] & 0x7F)

	// Validate before trusting the length, because the real danger here is not
	// an error — it is silent DESYNC. If the parser ever steps out of phase with
	// the stream it starts reading payload bytes as frame headers, and without
	// these checks that produces a plausible-looking header with a nonsense
	// length: the relay then blocks forever waiting for bytes that will never
	// come, on a call that was working fine. These turn that into a detectable
	// failure, which is what fail-open needs in order to trigger.
	//
	// RSV bits must be clear: they only carry meaning for a negotiated
	// extension, and the handshake refuses every extension offered.
	if first[0]&0x70 != 0 {
		return h, protocolError{"reserved bits set without a negotiated extension"}
	}
	switch h.opcode {
	case opContinuation, opText, opBinary, opClose, opPing, opPong:
	default:
		return h, protocolError{fmt.Sprintf("unknown opcode 0x%X", h.opcode)}
	}
	if h.isControl() {
		// RFC 6455 §5.5: control frames carry at most 125 bytes and may not be
		// fragmented.
		if h.length > 125 {
			return h, protocolError{"control frame longer than 125 bytes"}
		}
		if !h.fin {
			return h, protocolError{"fragmented control frame"}
		}
	}

	// 126 and 127 are escapes: the real length follows in 2 or 8 bytes.
	switch h.length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(src, ext[:]); err != nil {
			return h, err
		}
		if _, err := dst.Write(ext[:]); err != nil {
			return h, err
		}
		h.length = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(src, ext[:]); err != nil {
			return h, err
		}
		if _, err := dst.Write(ext[:]); err != nil {
			return h, err
		}
		length := binary.BigEndian.Uint64(ext[:])
		// The high bit must be 0 per RFC 6455; a peer setting it is malformed,
		// and int64 conversion would make it negative.
		if length > 1<<62 {
			return h, protocolError{fmt.Sprintf("frame length %d out of range", length)}
		}
		h.length = int64(length)
	}

	if h.masked {
		if _, err := io.ReadFull(src, h.maskKey[:]); err != nil {
			return h, err
		}
		if _, err := dst.Write(h.maskKey[:]); err != nil {
			return h, err
		}
	}
	return h, nil
}

// unmask applies the XOR mask in place. offset is how many payload bytes of this
// frame were already processed, since the mask cycles over the whole payload and
// a frame may be handled in chunks.
func unmask(payload []byte, key [4]byte, offset int64) {
	for i := range payload {
		payload[i] ^= key[(offset+int64(i))%4]
	}
}

// pump copies one direction of a WebSocket conversation, handing each complete
// data message to observe. It returns when the source ends.
//
// Text messages are buffered (up to maxBufferedMessageBytes) and delivered whole
// — reassembled across fragments, since a provider may split a JSON result and
// half a JSON object parses as nothing. Binary messages are streamed straight
// through and only their size is reported: they are audio, they are the bulk of
// a voice session, and nothing in them is readable anyway.
//
// Control frames are forwarded and ignored. A close frame is forwarded before
// returning, so the peer sees the shutdown the other side actually sent.
//
// FAIL OPEN. If a frame cannot be parsed, the rest of the connection is copied
// blind and metering for it stops. That is safe because every byte read is
// written onward BEFORE it is metered, so the peer's stream is byte-exact no
// matter how badly the parser lost the thread — the frames were only ever
// being read on the way past.
//
// This is the difference between metering being an observer and being a
// participant. A voice call must not drop because we failed to understand a
// frame; losing the cost row for that call is a far cheaper outcome, and it is
// visible in a way a dropped call in production is not.
type pumpConfig struct {
	dst io.Writer
	src io.Reader
	// buffer=false skips buffering entirely and reports sizes only, for a
	// direction whose content the meter does not need.
	buffer  bool
	observe func(extract.StreamMessage)
	// onDegrade is called if metering is abandoned mid-conversation. The
	// conversation itself continues.
	onDegrade func(error)
}

func pump(cfg pumpConfig) error {
	dst, src := cfg.dst, cfg.src
	buffer, observe := cfg.buffer, cfg.observe
	var (
		// Reassembly state for the data message currently in flight.
		msgOpcode   byte
		msgSize     int64
		msgBuf      []byte
		msgTruncate bool
	)

	flush := func() {
		if msgOpcode == 0 {
			return
		}
		observe(extract.StreamMessage{
			Text:   msgBuf,
			Binary: msgOpcode == opBinary,
			Size:   msgSize,
		})
		msgOpcode, msgSize, msgBuf, msgTruncate = 0, 0, nil, false
	}

	buf := make([]byte, copyChunkBytes)
	for {
		h, err := readFrameHeader(dst, src)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			var perr protocolError
			if errors.As(err, &perr) {
				if cfg.onDegrade != nil {
					cfg.onDegrade(err)
				}
				// Keep the conversation alive without metering it. Everything
				// read so far has already been forwarded, so resuming as a
				// blind copy leaves the peer's byte stream intact.
				_, copyErr := io.Copy(dst, src)
				if f, ok := dst.(interface{ Flush() error }); ok {
					_ = f.Flush()
				}
				return copyErr
			}
			return err
		}

		// Control frames carry no application data and may sit between the
		// fragments of a data message, so they never touch reassembly state.
		if h.isControl() {
			if _, err := io.CopyN(dst, src, h.length); err != nil {
				return err
			}
			if f, ok := dst.(interface{ Flush() error }); ok {
				_ = f.Flush()
			}
			if h.opcode == opClose {
				return nil
			}
			continue
		}

		if h.opcode != opContinuation {
			// A new data message begins. A previous unterminated one cannot be
			// completed, so drop it rather than merging two messages.
			msgOpcode, msgSize, msgBuf, msgTruncate = h.opcode, 0, nil, false
		}

		// Copy the payload through, buffering it only when it is text small
		// enough to be worth reading.
		var offset int64
		for remaining := h.length; remaining > 0; {
			n := int64(len(buf))
			if remaining < n {
				n = remaining
			}
			chunk := buf[:n]
			if _, err := io.ReadFull(src, chunk); err != nil {
				return err
			}
			if _, err := dst.Write(chunk); err != nil {
				return err
			}

			if buffer && msgOpcode == opText && !msgTruncate {
				if int64(len(msgBuf))+n > maxBufferedMessageBytes {
					// Too large to be provider control JSON. Keep forwarding it,
					// but do not meter a fragment of a message as if it were the
					// whole thing.
					msgBuf, msgTruncate = nil, true
				} else {
					plain := make([]byte, n)
					copy(plain, chunk)
					if h.masked {
						unmask(plain, h.maskKey, offset)
					}
					msgBuf = append(msgBuf, plain...)
				}
			}

			offset += n
			remaining -= n
			msgSize += n
		}

		// Data frames are flushed per frame, not per message: a provider that
		// fragments a response still expects each piece to arrive promptly.
		if f, ok := dst.(interface{ Flush() error }); ok {
			if err := f.Flush(); err != nil {
				return err
			}
		}

		if h.fin {
			flush()
		}
	}
}
