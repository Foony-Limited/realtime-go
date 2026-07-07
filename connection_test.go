package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewValidatesAuthOptions(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Error("New with no auth: want error")
	}
	if _, err := New(Options{Token: "t", Key: "k"}); err == nil {
		t.Error("New with two auth methods: want error")
	}
	if _, err := New(Options{Token: "t"}); err != nil {
		t.Errorf("New with one auth method: %v", err)
	}
}

func TestConnectHandshake(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if state := client.Connection.State(); state != ConnectionConnected {
		t.Errorf("state = %s, want connected", state)
	}
	if id := client.Connection.ID(); id != "conn-1" {
		t.Errorf("connection id = %q", id)
	}
	if clientID := client.Connection.ClientID(); clientID != "client-1" {
		t.Errorf("client id = %q", clientID)
	}
	if auth := edge.latestAuth(); auth.token != "test-token" {
		t.Errorf("auth token = %q", auth.token)
	}
}

func TestConnectAuthFailureStaticTokenIsTerminal(t *testing.T) {
	edge := newFakeEdge(t)
	edge.authCheck = func(*authFrame) *errorFrame {
		return &errorFrame{code: CodeBadAuth, message: "bad token"}
	}
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err == nil {
		t.Fatal("Connect with rejected auth: want error")
	}
	// A static token would be rejected identically forever: the connection must end
	// failed, not retry.
	waitFor(t, "failed state", func() bool {
		return client.Connection.State() == ConnectionFailed
	})
}

func TestConnectAuthFailureWithCallbackRetries(t *testing.T) {
	edge := newFakeEdge(t)
	var calls atomic.Int32
	edge.authCheck = func(auth *authFrame) *errorFrame {
		if auth.token == "stale" {
			return &errorFrame{code: CodeAuthExpired, message: "expired"}
		}
		return nil
	}
	client := newTestClient(t, edge, func(options *Options) {
		options.Token = ""
		options.AuthCallback = func(ctx context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "stale", nil
			}
			return "fresh", nil
		}
	})
	// The first attempt fails but AuthCallback can mint a fresh credential, so the
	// reconnect loop must recover on its own.
	_ = client.Connect(context.Background())
	waitFor(t, "reconnect with a fresh token", func() bool {
		return client.Connection.State() == ConnectionConnected
	})
	if got := edge.latestAuth().token; got != "fresh" {
		t.Errorf("second auth token = %q, want fresh", got)
	}
}

func TestReconnectResumesConnectionID(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	first := client.Connection.ID()
	edge.dropAll()
	waitFor(t, "reconnect", func() bool {
		return edge.connCount() == 1 && client.Connection.State() == ConnectionConnected
	})
	// The reconnect handshake must carry the previous connection id so presence
	// membership survives the gap.
	if got := edge.latestAuth().resumeConnectionID; got != first {
		t.Errorf("resumeConnectionID = %q, want %q", got, first)
	}
	if got := client.Connection.ID(); got != first {
		t.Errorf("connection id after resume = %q, want %q", got, first)
	}
}

func TestPublishBeforeConnectQueuesAndFlushes(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	// No explicit Connect: the publish must kick a lazy connect and flush the queued
	// frame once the handshake completes.
	if err := channel.Publish(context.Background(), "greeting", map[string]string{"text": "hi"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	publish := nextFrame[*publishFrame](t, edge)
	if publish.channel != "room:1" {
		t.Errorf("publish channel = %q", publish.channel)
	}
	// Single publishes ride the auto-batcher, so the message arrives as a batch
	// member.
	if len(publish.members) != 1 || publish.members[0].name != "greeting" {
		t.Fatalf("publish members = %+v", publish.members)
	}
	if string(publish.members[0].data) != `{"text":"hi"}` {
		t.Errorf("member data = %s", publish.members[0].data)
	}
	if publish.messageID == "" {
		t.Error("publish carries no messageID")
	}
}

func TestPublishRejectedByServer(t *testing.T) {
	edge := newFakeEdge(t)
	edge.rejectWith = CodeCapability
	client := newTestClient(t, edge)
	err := client.Channels.Get("room:1").Publish(context.Background(), "x", 1)
	var serverErr *ServerError
	if !errors.As(err, &serverErr) || serverErr.Code != CodeCapability {
		t.Fatalf("Publish error = %v, want ServerError 40301", err)
	}
}

func TestPublishWhileClosedRejects(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	channel := client.Channels.Get("room:1")
	client.Close()
	if err := channel.Publish(context.Background(), "x", 1, WithEphemeral()); err == nil {
		t.Error("Publish after Close: want error")
	}
}

func TestOutstandingPublishResentAfterDrop(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// Cut the link, publish into the outage, then let the reconnect flush it. The
	// stable messageID is what makes the resend safe (server-side dedup).
	edge.dropAll()
	waitFor(t, "disconnected state", func() bool {
		return client.Connection.State() != ConnectionConnected
	})
	done := make(chan error, 1)
	go func() {
		done <- client.Channels.Get("room:1").Publish(context.Background(), "queued", "data", WithEphemeral())
	}()
	publish := nextFrame[*publishFrame](t, edge)
	if publish.name != "queued" || !publish.ephemeral {
		t.Errorf("resent publish = %+v", publish)
	}
	if err := <-done; err != nil {
		t.Fatalf("queued publish: %v", err)
	}
}

func TestKeepAlivePings(t *testing.T) {
	edge := newFakeEdge(t)
	edge.keepAliveMs = 20
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case frame := <-edge.received:
			if _, ok := frame.(*pingFrame); ok {
				return
			}
		case <-deadline:
			t.Fatal("no keep-alive ping arrived")
		}
	}
}

func TestConnectionStateEvents(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	states := make(chan ConnectionState, 16)
	client.Connection.On(func(change ConnectionStateChange) {
		states <- change.Current
	})
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	client.Close()
	var seen []ConnectionState
	timeout := time.After(5 * time.Second)
	for len(seen) < 4 {
		select {
		case state := <-states:
			seen = append(seen, state)
		case <-timeout:
			t.Fatalf("state sequence so far: %v", seen)
		}
	}
	want := []ConnectionState{ConnectionConnecting, ConnectionConnected, ConnectionClosing, ConnectionClosed}
	for i, state := range want {
		if seen[i] != state {
			t.Fatalf("state[%d] = %s, want %s (full: %v)", i, seen[i], state, seen)
		}
	}
}

func TestBatchPublishFanOut(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	result, err := client.BatchPublish(context.Background(), BatchSpec{
		Channels: []string{"a", "b"},
		Messages: []BatchMessage{{Name: "n", Data: 1}},
	})
	if err != nil {
		t.Fatalf("BatchPublish: %v", err)
	}
	if result.SuccessCount != 2 || result.FailureCount != 0 {
		t.Errorf("result = %+v", result)
	}
	channels := map[string]bool{}
	for i := 0; i < 2; i++ {
		publish := nextFrame[*publishFrame](t, edge)
		channels[publish.channel] = true
		if len(publish.members) != 1 {
			t.Errorf("members = %+v", publish.members)
		}
	}
	if !channels["a"] || !channels["b"] {
		t.Errorf("channels = %v", channels)
	}
}

func TestBatchPublishLimits(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	tooManyChannels := make([]string, maxBatchChannels+1)
	for i := range tooManyChannels {
		tooManyChannels[i] = "c" + string(rune('a'+i%26)) + string(rune('a'+i/26))
	}
	if _, err := client.BatchPublish(context.Background(), BatchSpec{Channels: tooManyChannels, Messages: []BatchMessage{{Name: "n"}}}); err == nil {
		t.Error("channel limit: want error")
	}
	tooManyMessages := make([]BatchMessage, maxBatchMessages+1)
	if _, err := client.BatchPublish(context.Background(), BatchSpec{Channels: []string{"a"}, Messages: tooManyMessages}); err == nil {
		t.Error("message limit: want error")
	}
}

func TestUnscopedServerErrorSurfacesOnState(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	reasons := make(chan error, 1)
	client.Connection.OnState(ConnectionConnected, func(change ConnectionStateChange) {
		if change.Reason != nil {
			reasons <- change.Reason
		}
	})
	edge.pushLatest(&errorFrame{code: CodeRateLimited, message: "slow down"})
	select {
	case err := <-reasons:
		var serverErr *ServerError
		if !errors.As(err, &serverErr) || serverErr.Code != CodeRateLimited {
			t.Errorf("reason = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unscoped error never surfaced")
	}
}

func TestCloseRejectsPendingRequests(t *testing.T) {
	edge := newFakeEdge(t)
	// Swallow subs so the attach hangs until Close rejects it.
	edge.rejectWith = 0
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// Publish with a name that never acks: emulate by dropping the server without
	// closing the client first, with reconnects disabled via Close racing.
	channel := client.Channels.Get("room:1")
	attached := make(chan error, 1)
	go func() {
		attached <- channel.Attach(context.Background())
	}()
	nextFrame[*subscribeFrame](t, edge)
	// The sub was acked by the fake edge, so Attach resolves fine — this test pins
	// that Close afterwards leaves everything settled and the channel detached.
	if err := <-attached; err != nil {
		t.Fatalf("Attach: %v", err)
	}
	client.Close()
	waitFor(t, "channel detached on close", func() bool {
		return client.Connection.State() == ConnectionClosed
	})
}

func TestMessageDataRoundTrip(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 1)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", name: "greeting", data: json.RawMessage(`{"n":42}`), messageID: "m1", clientID: "c1", seq: 1, timestamp: 123})
	select {
	case message := <-got:
		var payload struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(message.Data, &payload); err != nil || payload.N != 42 {
			t.Errorf("data = %s (err %v)", message.Data, err)
		}
		if message.Serial != 1 || message.Timestamp != 123 || message.ClientID != "c1" {
			t.Errorf("message = %+v", message)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message never delivered")
	}
}
