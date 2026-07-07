package realtime

// An in-process fake edge for tests: a real WebSocket server speaking the binary
// opcode protocol via the same codec, so connection/channel/presence behavior is
// exercised end to end over a live socket.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeEdge is one fake realtime edge. Tests read the client frames it receives from
// received, and push server frames with push / pushLatest.
type fakeEdge struct {
	t      *testing.T
	server *httptest.Server

	// received carries every client frame after auth, in arrival order.
	received chan any

	mu    sync.Mutex
	conns []*edgeConn
	// nextConnID names connections conn-1, conn-2, ...
	nextConnID int
	nextSerial uint64

	// authCheck, when set, can reject the handshake with an error frame.
	authCheck func(*authFrame) *errorFrame
	// keepAliveMs is advertised on the connected frame. 0 disables client pings.
	keepAliveMs uint64
	// onSubscribe, when set, overrides the resumed flag of a resume sub's ack.
	resumedValue bool
	// ackSeq, when true, acks publishes with an incrementing serial.
	ackSeq bool
	// onHistory / onFetch answer hist / fetch frames. nil answers empty.
	onHistory func(*historyFrame) *historyResponseFrame
	onFetch   func(*fetchFrame) *fetchResponseFrame
	// rejectWith, when set, answers every acked request with this error code.
	rejectWith int
}

// edgeConn is one accepted connection.
type edgeConn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex
	id      string
	auth    *authFrame
}

func newFakeEdge(t *testing.T) *fakeEdge {
	edge := &fakeEdge{
		t:           t,
		received:    make(chan any, 256),
		keepAliveMs: 60_000,
	}
	edge.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		edge.serve(ws)
	}))
	t.Cleanup(edge.server.Close)
	return edge
}

// endpoint returns the URL to point Options.Endpoint at.
func (e *fakeEdge) endpoint() string {
	return e.server.URL
}

func (e *fakeEdge) serve(ws *websocket.Conn) {
	conn := &edgeConn{ws: ws}
	for {
		messageType, data, err := ws.Read(context.Background())
		if err != nil {
			return
		}
		if messageType != websocket.MessageBinary {
			continue
		}
		for _, record := range splitBinaryRecords(data) {
			frame, err := decodeClientRecord(record)
			if err != nil {
				e.t.Errorf("fake edge: bad client record: %v", err)
				continue
			}
			e.handle(conn, frame)
		}
	}
}

func (e *fakeEdge) handle(conn *edgeConn, frame any) {
	switch f := frame.(type) {
	case *authFrame:
		conn.auth = f
		if e.authCheck != nil {
			if errFrame := e.authCheck(f); errFrame != nil {
				e.write(conn, errFrame)
				return
			}
		}
		e.mu.Lock()
		if f.resumeConnectionID != "" {
			conn.id = f.resumeConnectionID
		} else {
			e.nextConnID++
			conn.id = "conn-" + strconv.Itoa(e.nextConnID)
		}
		e.conns = append(e.conns, conn)
		e.mu.Unlock()
		clientID := f.clientID
		if clientID == "" {
			clientID = "client-1"
		}
		e.write(conn, &connectedFrame{connectionID: conn.id, keepAliveMs: e.keepAliveMs, clientID: clientID})
	case *pingFrame:
		e.received <- f
		e.write(conn, &pongFrame{})
	case *subscribeFrame:
		e.received <- f
		if e.rejectWith != 0 {
			e.write(conn, &errorFrame{id: f.id, code: e.rejectWith, message: "rejected"})
			return
		}
		ack := &ackFrame{id: f.id}
		if f.lastSerial > 0 {
			resumed := e.resumedValue
			ack.resumed = &resumed
		}
		e.write(conn, ack)
	case *unsubscribeFrame:
		e.received <- f
		e.write(conn, &ackFrame{id: f.id})
	case *publishFrame:
		e.received <- f
		if e.rejectWith != 0 {
			e.write(conn, &errorFrame{id: f.id, code: e.rejectWith, message: "rejected"})
			return
		}
		ack := &ackFrame{id: f.id}
		if e.ackSeq {
			e.mu.Lock()
			e.nextSerial++
			ack.seq = e.nextSerial
			e.mu.Unlock()
		}
		e.write(conn, ack)
	case *presenceFrame, *presenceSubscribeFrame, *presenceUnsubscribeFrame:
		e.received <- f
		e.write(conn, &ackFrame{id: frameID(f)})
	case *historyFrame:
		e.received <- f
		response := &historyResponseFrame{id: f.id, channel: f.channel}
		if e.onHistory != nil {
			response = e.onHistory(f)
			response.id = f.id
		}
		e.write(conn, response)
	case *fetchFrame:
		e.received <- f
		response := &fetchResponseFrame{id: f.id, channel: f.channel, resumed: true}
		if e.onFetch != nil {
			response = e.onFetch(f)
			response.id = f.id
		}
		e.write(conn, response)
	}
}

// frameID reads the request id off the ackable presence frames.
func frameID(frame any) uint64 {
	switch f := frame.(type) {
	case *presenceFrame:
		return f.id
	case *presenceSubscribeFrame:
		return f.id
	case *presenceUnsubscribeFrame:
		return f.id
	}
	return 0
}

// write encodes and sends one server frame on conn.
func (e *fakeEdge) write(conn *edgeConn, frame any) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()
	_ = conn.ws.Write(ctx, websocket.MessageBinary, frameBinaryRecord(encodeServerFrame(frame)))
}

// pushLatest sends a server frame on the most recent connection.
func (e *fakeEdge) pushLatest(frame any) {
	e.mu.Lock()
	conn := e.conns[len(e.conns)-1]
	e.mu.Unlock()
	e.write(conn, frame)
}

// dropAll closes every accepted connection from the server side.
func (e *fakeEdge) dropAll() {
	e.mu.Lock()
	conns := e.conns
	e.conns = nil
	e.mu.Unlock()
	for _, conn := range conns {
		_ = conn.ws.Close(websocket.StatusGoingAway, "test drop")
	}
}

// connCount returns how many connections completed auth.
func (e *fakeEdge) connCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.conns)
}

// latestAuth returns the auth frame of the most recent connection.
func (e *fakeEdge) latestAuth() *authFrame {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.conns[len(e.conns)-1].auth
}

// nextFrame waits for the next client frame of type T, skipping pings and failing the
// test after a timeout.
func nextFrame[T any](t *testing.T, edge *fakeEdge) T {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case frame := <-edge.received:
			if _, isPing := frame.(*pingFrame); isPing {
				continue
			}
			typed, ok := frame.(T)
			if !ok {
				t.Fatalf("next frame: got %T, want %T", frame, *new(T))
			}
			return typed
		case <-deadline:
			t.Fatalf("timed out waiting for a %T", *new(T))
			panic("unreachable")
		}
	}
}

// waitFor polls until check passes or the timeout elapses.
func waitFor(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// newTestClient builds a client against edge with fast reconnects.
func newTestClient(t *testing.T, edge *fakeEdge, mutate ...func(*Options)) *Client {
	t.Helper()
	options := Options{
		Endpoint:              edge.endpoint(),
		Token:                 "test-token",
		InitialReconnectDelay: 5 * time.Millisecond,
		MaxReconnectDelay:     20 * time.Millisecond,
	}
	for _, fn := range mutate {
		fn(&options)
	}
	client, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}
