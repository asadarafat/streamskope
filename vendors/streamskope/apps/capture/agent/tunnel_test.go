package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestTunnelTransfersBinaryFramesAndReleasesConnection(t *testing.T) {
	t.Parallel()

	broker := startEchoBroker(t)
	handler := NewTunnelHandler(
		TunnelLimits{IdleTimeout: time.Second, MaxConnections: 2, MaxFrameBytes: 1024, WriteTimeout: time.Second},
		func(_ context.Context, sessionID string) (string, error) {
			if sessionID != "7ac7d717-8ef0-4a55-bd33-5b723e6b6918" {
				t.Fatalf("unexpected session id: %s", sessionID)
			}
			return broker, nil
		},
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	connection, reader := openWebSocket(t, server.URL, "/v1/sessions/7ac7d717-8ef0-4a55-bd33-5b723e6b6918/tunnel")
	payload := []byte{0, 1, 2, 3, 255}
	writeClientFrame(t, connection, payload)
	if received := readServerFrame(t, reader); !bytes.Equal(received, payload) {
		t.Fatalf("received %v, want %v", received, payload)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for handler.ActiveConnections() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if handler.ActiveConnections() != 0 {
		t.Fatalf("active connections = %d, want 0", handler.ActiveConnections())
	}
}

func TestTunnelRejectsOversizedFrame(t *testing.T) {
	t.Parallel()

	broker := startEchoBroker(t)
	handler := NewTunnelHandler(
		TunnelLimits{IdleTimeout: time.Second, MaxConnections: 1, MaxFrameBytes: 4, WriteTimeout: time.Second},
		func(context.Context, string) (string, error) { return broker, nil },
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	first, firstReader := openWebSocket(t, server.URL, "/v1/sessions/7ac7d717-8ef0-4a55-bd33-5b723e6b6918/tunnel")
	defer first.Close()
	writeClientFrame(t, first, []byte("12345"))
	if code := readServerCloseCode(t, firstReader); code != 1009 {
		t.Fatalf("close code = %d, want 1009", code)
	}
}

func TestTunnelRejectsConnectionOverflow(t *testing.T) {
	t.Parallel()

	broker := startEchoBroker(t)
	handler := NewTunnelHandler(
		TunnelLimits{IdleTimeout: time.Second, MaxConnections: 1, MaxFrameBytes: 4, WriteTimeout: time.Second},
		func(context.Context, string) (string, error) { return broker, nil },
	)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	blocking, _ := openWebSocket(t, server.URL, "/v1/sessions/7ac7d717-8ef0-4a55-bd33-5b723e6b6918/tunnel")
	defer blocking.Close()
	response, err := http.Get(server.URL + "/v1/sessions/290b9378-10a6-4739-a69a-0058e8fb936f/tunnel")
	if err != nil {
		t.Fatalf("overflow request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 503 {
		t.Fatalf("overflow status = %d, want 503", response.StatusCode)
	}
}

func startEchoBroker(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, acceptError := listener.Accept()
			if acceptError != nil {
				return
			}
			go func() {
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()
	return listener.Addr().String()
}

func openWebSocket(t *testing.T, serverURL string, path string) (net.Conn, *bufio.Reader) {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	connection, err := net.DialTimeout("tcp", parsed.Host, time.Second)
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	request := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + parsed.Host + "\r\n" +
		"Connection: Upgrade\r\n" +
		"Upgrade: websocket\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write upgrade: %v", err)
	}
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read upgrade status: %v", err)
	}
	if !strings.Contains(status, "101") {
		_ = connection.Close()
		t.Fatalf("upgrade status %q, want 101", strings.TrimSpace(status))
	}
	for {
		line, readError := reader.ReadString('\n')
		if readError != nil {
			t.Fatalf("read upgrade headers: %v", readError)
		}
		if line == "\r\n" {
			break
		}
	}
	return connection, reader
}

func writeClientFrame(t *testing.T, writer io.Writer, payload []byte) {
	t.Helper()
	if len(payload) > 125 {
		t.Fatal("test helper supports payloads up to 125 bytes")
	}
	mask := [4]byte{1, 2, 3, 4}
	frame := []byte{0x82, 0x80 | byte(len(payload)), mask[0], mask[1], mask[2], mask[3]}
	for index, value := range payload {
		frame = append(frame, value^mask[index%len(mask)])
	}
	if _, err := writer.Write(frame); err != nil {
		t.Fatalf("write websocket frame: %v", err)
	}
}

func readServerFrame(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		t.Fatalf("read websocket frame: %v", err)
	}
	if header[0] != 0x82 || header[1]&0x80 != 0 {
		t.Fatalf("unexpected websocket frame header: %v", header)
	}
	length := int(header[1] & 0x7f)
	if length == 126 {
		extended := make([]byte, 2)
		if _, err := io.ReadFull(reader, extended); err != nil {
			t.Fatalf("read extended frame length: %v", err)
		}
		length = int(binary.BigEndian.Uint16(extended))
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("read websocket payload: %v", err)
	}
	return payload
}

func readServerCloseCode(t *testing.T, reader *bufio.Reader) uint16 {
	t.Helper()
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		t.Fatalf("read close frame: %v", err)
	}
	if header[0]&0x0f != 0x08 {
		t.Fatalf("unexpected close opcode: %d", header[0]&0x0f)
	}
	payload := make([]byte, int(header[1]&0x7f))
	if _, err := io.ReadFull(reader, payload); err != nil {
		t.Fatalf("read close payload: %v", err)
	}
	if len(payload) < 2 {
		t.Fatal("close frame has no status code")
	}
	return binary.BigEndian.Uint16(payload[:2])
}
