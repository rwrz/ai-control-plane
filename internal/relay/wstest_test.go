package relay

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// A minimal, independent RFC 6455 implementation for tests.
//
// It is written from the spec rather than reusing the relay's own frame code on
// purpose: a test that shares the parser under test agrees with it by
// construction, including where both are wrong. Here the relay is exercised
// against frames built by something that does not know how it works — masked
// client frames, fragmented messages, interleaved control frames — which is what
// a real SDK sends.

// wsWriteFrame writes one frame. Client frames must be masked, server frames
// must not — the relay has to handle both, in both directions.
func wsWriteFrame(w io.Writer, fin bool, opcode byte, payload []byte, mask bool) error {
	var head []byte

	first := opcode
	if fin {
		first |= 0x80
	}
	head = append(head, first)

	maskBit := byte(0)
	if mask {
		maskBit = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		head = append(head, maskBit|byte(n))
	case n < 1<<16:
		head = append(head, maskBit|126)
		head = binary.BigEndian.AppendUint16(head, uint16(n))
	default:
		head = append(head, maskBit|127)
		head = binary.BigEndian.AppendUint64(head, uint64(n))
	}

	body := append([]byte(nil), payload...)
	if mask {
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		head = append(head, key[:]...)
		for i := range body {
			body[i] ^= key[i%4]
		}
	}

	if _, err := w.Write(append(head, body...)); err != nil {
		return err
	}
	if f, ok := w.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// wsReadMessage reads one complete message, reassembling fragments and skipping
// control frames. Returns the opcode of the data message and its payload.
func wsReadMessage(r io.Reader) (byte, []byte, error) {
	var (
		msgOpcode byte
		payload   []byte
	)
	for {
		var first [2]byte
		if _, err := io.ReadFull(r, first[:]); err != nil {
			return 0, nil, err
		}
		fin := first[0]&0x80 != 0
		opcode := first[0] & 0x0F
		masked := first[1]&0x80 != 0
		length := int64(first[1] & 0x7F)

		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(r, ext[:]); err != nil {
				return 0, nil, err
			}
			length = int64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(r, ext[:]); err != nil {
				return 0, nil, err
			}
			length = int64(binary.BigEndian.Uint64(ext[:]))
		}

		var key [4]byte
		if masked {
			if _, err := io.ReadFull(r, key[:]); err != nil {
				return 0, nil, err
			}
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range body {
				body[i] ^= key[i%4]
			}
		}

		if opcode&0x8 != 0 { // control frame
			if opcode == opClose {
				return opClose, body, nil
			}
			continue
		}
		if opcode != opContinuation {
			msgOpcode = opcode
		}
		payload = append(payload, body...)
		if fin {
			return msgOpcode, payload, nil
		}
	}
}

// wsConn is one side of a test WebSocket conversation.
type wsConn struct {
	conn net.Conn
	rw   *bufio.ReadWriter
	mask bool // true for the client side
}

func (c *wsConn) writeText(payload string) error {
	return wsWriteFrame(c.rw, true, opText, []byte(payload), c.mask)
}

func (c *wsConn) writeBinary(payload []byte) error {
	return wsWriteFrame(c.rw, true, opBinary, payload, c.mask)
}

// writeFragmentedText splits a message across two frames with a ping in
// between, which is legal and is what reassembly has to survive.
func (c *wsConn) writeFragmentedText(a, b string) error {
	if err := wsWriteFrame(c.rw, false, opText, []byte(a), c.mask); err != nil {
		return err
	}
	if err := wsWriteFrame(c.rw, true, opPing, []byte("hb"), c.mask); err != nil {
		return err
	}
	return wsWriteFrame(c.rw, true, opContinuation, []byte(b), c.mask)
}

func (c *wsConn) writeClose() error {
	return wsWriteFrame(c.rw, true, opClose, []byte{0x03, 0xE8}, c.mask)
}

func (c *wsConn) read() (byte, []byte, error) { return wsReadMessage(c.rw) }

func (c *wsConn) Close() error { return c.conn.Close() }

// wsHandshake performs the server half of the upgrade and returns the raw conn.
func wsHandshake(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		return nil, fmt.Errorf("missing Sec-WebSocket-Key")
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("no hijacker")
	}
	conn, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})

	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAcceptToken(key) + "\r\n\r\n"
	if _, err := rw.WriteString(resp); err != nil {
		conn.Close()
		return nil, err
	}
	if err := rw.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	return &wsConn{conn: conn, rw: rw, mask: false}, nil
}

// wsDial opens a client WebSocket to rawURL through the relay under test.
func wsDial(t *testing.T, rawURL string, headers http.Header) (*wsConn, *http.Response, error) {
	t.Helper()

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "80")
	}
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		return nil, nil, err
	}

	var rawKey [16]byte
	if _, err := rand.Read(rawKey[:]); err != nil {
		conn.Close()
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(rawKey[:])
	requestURI := u.RequestURI()

	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\nHost: %s\r\n", requestURI, u.Host)
	b.WriteString("Upgrade: websocket\r\nConnection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Version: 13\r\n")
	fmt.Fprintf(&b, "Sec-WebSocket-Key: %s\r\n", key)
	for name, values := range headers {
		for _, v := range values {
			fmt.Fprintf(&b, "%s: %s\r\n", name, v)
		}
	}
	b.WriteString("\r\n")

	if _, err := conn.Write([]byte(b.String())); err != nil {
		conn.Close()
		return nil, nil, err
	}

	reader := bufio.NewReader(conn)
	req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return nil, resp, nil
	}
	// Validate the challenge response, exactly as a real client does.
	//
	// This is not ceremony. The relay dials upstream with a fresh key of its
	// own, so the provider's Sec-WebSocket-Accept answers a challenge THIS
	// client never issued; forwarding it verbatim produces a 101 that every
	// conforming client rejects. Without this check the whole suite passed
	// while no real SDK could connect.
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wsAcceptToken(key) {
		conn.Close()
		return nil, resp, fmt.Errorf(
			"Sec-WebSocket-Accept = %q, want %q (derived from THIS client's key)",
			got, wsAcceptToken(key))
	}
	return &wsConn{
		conn: conn,
		rw:   bufio.NewReadWriter(reader, bufio.NewWriter(conn)),
		mask: true,
	}, resp, nil
}
