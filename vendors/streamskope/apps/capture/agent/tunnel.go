package agent

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const webSocketMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

var (
	errFrameTooLarge = errors.New("websocket frame exceeds the configured limit")
	errProtocol      = errors.New("invalid websocket frame")
)

type TunnelLimits struct {
	IdleTimeout    time.Duration
	MaxConnections int
	MaxFrameBytes  int64
	WriteTimeout   time.Duration
}

type BrokerResolver func(context.Context, string) (string, error)

type TunnelHandler struct {
	active   atomic.Int64
	dialer   net.Dialer
	limits   TunnelLimits
	resolver BrokerResolver
	slots    chan struct{}
}

func NewTunnelHandler(limits TunnelLimits, resolver BrokerResolver) *TunnelHandler {
	if limits.IdleTimeout <= 0 {
		limits.IdleTimeout = 60 * time.Second
	}
	if limits.MaxConnections <= 0 {
		limits.MaxConnections = 8
	}
	if limits.MaxFrameBytes <= 0 {
		limits.MaxFrameBytes = 16 * 1024 * 1024
	}
	if limits.WriteTimeout <= 0 {
		limits.WriteTimeout = 10 * time.Second
	}
	return &TunnelHandler{
		dialer:   net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second},
		limits:   limits,
		resolver: resolver,
		slots:    make(chan struct{}, limits.MaxConnections),
	}
}

func (handler *TunnelHandler) ActiveConnections() int64 {
	return handler.active.Load()
}

func (handler *TunnelHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	sessionID, validPath := tunnelSessionID(request.URL.Path)
	if request.Method != http.MethodGet || !validPath {
		http.NotFound(response, request)
		return
	}
	select {
	case handler.slots <- struct{}{}:
		defer func() { <-handler.slots }()
	default:
		http.Error(response, "capture tunnel capacity reached", http.StatusServiceUnavailable)
		return
	}
	if !headerHasToken(request.Header, "Connection", "upgrade") || !strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
		http.Error(response, "websocket upgrade required", http.StatusUpgradeRequired)
		return
	}
	key := strings.TrimSpace(request.Header.Get("Sec-WebSocket-Key"))
	if key == "" || request.Header.Get("Sec-WebSocket-Version") != "13" {
		http.Error(response, "invalid websocket upgrade", http.StatusBadRequest)
		return
	}
	brokerAddress, err := handler.resolver(request.Context(), sessionID)
	if err != nil {
		http.Error(response, "capture session is unavailable", http.StatusNotFound)
		return
	}
	broker, err := handler.dialer.DialContext(request.Context(), "tcp", brokerAddress)
	if err != nil {
		http.Error(response, "capture broker is unavailable", http.StatusBadGateway)
		return
	}
	defer broker.Close()

	hijacker, ok := response.(http.Hijacker)
	if !ok {
		http.Error(response, "websocket upgrade is unavailable", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	defer client.Close()
	accept := sha1.Sum([]byte(key + webSocketMagic))
	if _, err := fmt.Fprintf(
		buffered,
		"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
		base64.StdEncoding.EncodeToString(accept[:]),
	); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}

	handler.active.Add(1)
	defer handler.active.Add(-1)
	writer := &webSocketWriter{connection: client, timeout: handler.limits.WriteTimeout}
	errorsChannel := make(chan error, 2)
	go func() {
		errorsChannel <- handler.copyClientToBroker(buffered.Reader, client, broker, writer)
	}()
	go func() {
		errorsChannel <- handler.copyBrokerToClient(broker, client, writer)
	}()
	copyError := <-errorsChannel
	if errors.Is(copyError, errFrameTooLarge) {
		_ = writer.close(1009)
	} else if errors.Is(copyError, errProtocol) {
		_ = writer.close(1002)
	}
}

func tunnelSessionID(path string) (string, bool) {
	const prefix = "/v1/sessions/"
	const suffix = "/tunnel"
	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	if strings.Contains(value, "/") || !uuidPattern.MatchString(value) {
		return "", false
	}
	return value, true
}

func headerHasToken(header http.Header, name string, expected string) bool {
	for _, part := range strings.Split(header.Get(name), ",") {
		if strings.EqualFold(strings.TrimSpace(part), expected) {
			return true
		}
	}
	return false
}

func (handler *TunnelHandler) copyClientToBroker(reader *bufio.Reader, client net.Conn, broker net.Conn, writer *webSocketWriter) error {
	var message []byte
	for {
		if err := client.SetReadDeadline(time.Now().Add(handler.limits.IdleTimeout)); err != nil {
			return err
		}
		frame, err := readClientFrame(reader, handler.limits.MaxFrameBytes-int64(len(message)))
		if err != nil {
			return err
		}
		switch frame.opcode {
		case 0x0:
			if message == nil {
				return errProtocol
			}
			message = append(message, frame.payload...)
		case 0x2:
			if message != nil {
				return errProtocol
			}
			message = append(message, frame.payload...)
		case 0x8:
			return io.EOF
		case 0x9:
			if err := writer.frame(0xA, frame.payload); err != nil {
				return err
			}
			continue
		case 0xA:
			continue
		default:
			return errProtocol
		}
		if !frame.final {
			continue
		}
		if err := broker.SetWriteDeadline(time.Now().Add(handler.limits.WriteTimeout)); err != nil {
			return err
		}
		if _, err := broker.Write(message); err != nil {
			return err
		}
		message = nil
	}
}

func (handler *TunnelHandler) copyBrokerToClient(broker net.Conn, client net.Conn, writer *webSocketWriter) error {
	buffer := make([]byte, 32*1024)
	for {
		if err := broker.SetReadDeadline(time.Now().Add(handler.limits.IdleTimeout)); err != nil {
			return err
		}
		read, err := broker.Read(buffer)
		if read > 0 {
			if err := client.SetWriteDeadline(time.Now().Add(handler.limits.WriteTimeout)); err != nil {
				return err
			}
			if writeError := writer.frame(0x2, buffer[:read]); writeError != nil {
				return writeError
			}
		}
		if err != nil {
			return err
		}
	}
}

type webSocketFrame struct {
	final   bool
	opcode  byte
	payload []byte
}

func readClientFrame(reader io.Reader, maximum int64) (webSocketFrame, error) {
	if maximum < 0 {
		return webSocketFrame{}, errFrameTooLarge
	}
	header := make([]byte, 2)
	if _, err := io.ReadFull(reader, header); err != nil {
		return webSocketFrame{}, err
	}
	if header[0]&0x70 != 0 || header[1]&0x80 == 0 {
		return webSocketFrame{}, errProtocol
	}
	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(reader, extended); err != nil {
			return webSocketFrame{}, err
		}
		length = uint64(binary.BigEndian.Uint16(extended))
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(reader, extended); err != nil {
			return webSocketFrame{}, err
		}
		length = binary.BigEndian.Uint64(extended)
	}
	if length > uint64(maximum) {
		return webSocketFrame{}, errFrameTooLarge
	}
	mask := make([]byte, 4)
	if _, err := io.ReadFull(reader, mask); err != nil {
		return webSocketFrame{}, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return webSocketFrame{}, err
	}
	for index := range payload {
		payload[index] ^= mask[index%len(mask)]
	}
	return webSocketFrame{final: header[0]&0x80 != 0, opcode: header[0] & 0x0f, payload: payload}, nil
}

type webSocketWriter struct {
	connection net.Conn
	mutex      sync.Mutex
	timeout    time.Duration
}

func (writer *webSocketWriter) close(code uint16) error {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, code)
	return writer.frame(0x8, payload)
}

func (writer *webSocketWriter) frame(opcode byte, payload []byte) error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if err := writer.connection.SetWriteDeadline(time.Now().Add(writer.timeout)); err != nil {
		return err
	}
	header := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		header = append(header, byte(len(payload)))
	case len(payload) <= 65535:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(len(payload)))
	default:
		header = append(header, 127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(header[len(header)-8:], uint64(len(payload)))
	}
	if _, err := writer.connection.Write(header); err != nil {
		return err
	}
	_, err := writer.connection.Write(payload)
	return err
}
