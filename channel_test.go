package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestChannelsGetIsStablePerName(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	first := client.Channels.Get("room:1")
	if client.Channels.Get("room:1") != first {
		t.Error("same name returned a different instance")
	}
	client.Channels.Release("room:1")
	if client.Channels.Get("room:1") == first {
		t.Error("Get after Release returned the released instance")
	}
}

func TestChannelsGetRejectsBadNames(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	defer func() {
		if recover() == nil {
			t.Error("Get with a dotted name: want panic")
		}
	}()
	client.Channels.Get("bad.name")
}

func TestAttachDetachLifecycle(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	if state := channel.State(); state != ChannelInitialized {
		t.Errorf("initial state = %s", state)
	}
	if err := channel.Attach(context.Background()); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if state := channel.State(); state != ChannelAttached {
		t.Errorf("state after attach = %s", state)
	}
	sub := nextFrame[*subscribeFrame](t, edge)
	if sub.channel != "room:1" || sub.lastSerial != 0 {
		t.Errorf("sub = %+v", sub)
	}
	if err := channel.Detach(context.Background()); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	unsub := nextFrame[*unsubscribeFrame](t, edge)
	if unsub.channel != "room:1" {
		t.Errorf("unsub = %+v", unsub)
	}
	if state := channel.State(); state != ChannelDetached {
		t.Errorf("state after detach = %s", state)
	}
}

func TestAttachCapabilityDenialIsTerminal(t *testing.T) {
	edge := newFakeEdge(t)
	edge.rejectWith = CodeChannelDenied
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	err := channel.Attach(context.Background())
	var serverErr *ServerError
	if !errors.As(err, &serverErr) || serverErr.Code != CodeChannelDenied {
		t.Fatalf("Attach error = %v", err)
	}
	if state := channel.State(); state != ChannelFailed {
		t.Errorf("state = %s, want failed", state)
	}
	// Drain the rejected sub so the assertion below reads post-reconnect traffic.
	nextFrame[*subscribeFrame](t, edge)
	// A terminal denial must not be re-subscribed on reconnect.
	edge.rejectWith = 0
	edge.dropAll()
	waitFor(t, "reconnect", func() bool {
		return client.Connection.State() == ConnectionConnected && edge.connCount() == 1
	})
	select {
	case frame := <-edge.received:
		if _, ok := frame.(*subscribeFrame); ok {
			t.Error("failed channel was re-subscribed on reconnect")
		}
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSubscribeByName(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	var all, named []string
	gotAll := make(chan string, 8)
	gotNamed := make(chan string, 8)
	channel.Subscribe(func(message *Message) { gotAll <- message.Name })
	channel.Subscribe(func(message *Message) { gotNamed <- message.Name }, "wanted")
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", name: "other", messageID: "m1", seq: 1})
	edge.pushLatest(&msgFrame{channel: "room:1", name: "wanted", messageID: "m2", seq: 2})
	waitFor(t, "both catch-all deliveries", func() bool {
		for {
			select {
			case name := <-gotAll:
				all = append(all, name)
			case name := <-gotNamed:
				named = append(named, name)
			default:
				return len(all) == 2 && len(named) == 1
			}
		}
	})
	if all[0] != "other" || all[1] != "wanted" || named[0] != "wanted" {
		t.Errorf("all = %v, named = %v", all, named)
	}
}

func TestAutoBatchGroupsBurst(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	channel := client.Channels.Get("room:1", WithBatchOptions(BatchOptions{Interval: 40 * time.Millisecond}))
	// The first publish flushes immediately (no batch in the last Interval). The next
	// two land inside the throttle window and must group into ONE batch frame.
	if err := channel.Publish(context.Background(), "first", 1); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	done := make(chan error, 2)
	go func() { done <- channel.Publish(context.Background(), "second", 2) }()
	go func() { done <- channel.Publish(context.Background(), "third", 3) }()
	first := nextFrame[*publishFrame](t, edge)
	if len(first.members) != 1 || first.members[0].name != "first" {
		t.Fatalf("first frame members = %+v", first.members)
	}
	second := nextFrame[*publishFrame](t, edge)
	if len(second.members) != 2 {
		t.Fatalf("burst frame members = %+v", second.members)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("burst publish: %v", err)
		}
	}
}

func TestEphemeralPublishSkipsBatching(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	if err := channel.Publish(context.Background(), "cursor", 7, WithEphemeral()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	publish := nextFrame[*publishFrame](t, edge)
	if !publish.ephemeral || publish.name != "cursor" || len(publish.members) != 0 {
		t.Errorf("publish = %+v", publish)
	}
}

func TestPublishBatchAtomic(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	err := channel.PublishBatch(context.Background(), []BatchMessage{
		{Name: "a", Data: 1},
		{Name: "b", Data: "two"},
	})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	publish := nextFrame[*publishFrame](t, edge)
	if len(publish.members) != 2 || publish.members[1].name != "b" {
		t.Errorf("members = %+v", publish.members)
	}
}

func TestBatchDeliveryExpandsMembers(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 4)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", messageID: "bat", clientID: "c1", seq: 3, timestamp: 9, members: []wireMember{
		{name: "a", data: json.RawMessage(`1`)},
		{name: "b", data: json.RawMessage(`2`)},
	}})
	var messages []*Message
	waitFor(t, "batch members", func() bool {
		for {
			select {
			case message := <-got:
				messages = append(messages, message)
			default:
				return len(messages) == 2
			}
		}
	})
	if messages[0].ID != "bat:0" || messages[1].ID != "bat:1" {
		t.Errorf("member ids = %q, %q", messages[0].ID, messages[1].ID)
	}
	if messages[0].Name != "a" || messages[1].Name != "b" {
		t.Errorf("member names = %q, %q", messages[0].Name, messages[1].Name)
	}
}

func TestBundleDeliveryUnwrapsMembers(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 4)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", bundle: []bundledMessage{
		{name: "a", data: json.RawMessage(`1`), messageID: "m1", clientID: "c1", seq: 1},
		{name: "b", data: json.RawMessage(`2`), messageID: "m2", clientID: "c2", seq: 2},
	}})
	var messages []*Message
	waitFor(t, "bundle members", func() bool {
		for {
			select {
			case message := <-got:
				messages = append(messages, message)
			default:
				return len(messages) == 2
			}
		}
	})
	if messages[0].Name != "a" || messages[1].Name != "b" || messages[1].Serial != 2 {
		t.Errorf("messages = %+v, %+v", messages[0], messages[1])
	}
}

func TestDuplicateMessagesDropped(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 4)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	frame := &msgFrame{channel: "room:1", name: "x", messageID: "m1", clientID: "c1", seq: 1}
	edge.pushLatest(frame)
	edge.pushLatest(frame)
	// A different client reusing the same message id must NOT be suppressed.
	edge.pushLatest(&msgFrame{channel: "room:1", name: "x", messageID: "m1", clientID: "c2", seq: 2})
	var count int
	waitFor(t, "two deliveries", func() bool {
		for {
			select {
			case <-got:
				count++
			default:
				return count == 2
			}
		}
	})
	select {
	case <-got:
		t.Error("duplicate was delivered")
	case <-time.After(30 * time.Millisecond):
	}
}

func TestSerialGapTriggersSurgicalFetch(t *testing.T) {
	edge := newFakeEdge(t)
	edge.onFetch = func(fetch *fetchFrame) *fetchResponseFrame {
		return &fetchResponseFrame{
			channel: fetch.channel,
			resumed: true,
			messages: []msgFrame{
				{channel: fetch.channel, name: "six", messageID: "m6", clientID: "c", seq: 6},
				{channel: fetch.channel, name: "seven", messageID: "m7", clientID: "c", seq: 7},
			},
		}
	}
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 8)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	// Serial 5 baselines the cursor. Serial 7 is a gap (6 was lost) and must trigger
	// a fetch from 5, not a re-subscribe.
	edge.pushLatest(&msgFrame{channel: "room:1", name: "five", messageID: "m5", clientID: "c", seq: 5})
	edge.pushLatest(&msgFrame{channel: "room:1", name: "seven", messageID: "m7", clientID: "c", seq: 7})
	fetch := nextFrame[*fetchFrame](t, edge)
	if fetch.fromSerial != 5 {
		t.Errorf("fetch fromSerial = %d, want 5", fetch.fromSerial)
	}
	var names []string
	waitFor(t, "gap healed", func() bool {
		for {
			select {
			case message := <-got:
				names = append(names, message.Name)
			default:
				return len(names) == 3
			}
		}
	})
	// Live order is five then seven, and the fetch replays six (seven dedups away).
	if names[0] != "five" || names[1] != "seven" || names[2] != "six" {
		t.Errorf("names = %v", names)
	}
	if cursor := channel.cursor(); cursor != 7 {
		t.Errorf("cursor after heal = %d, want 7", cursor)
	}
}

func TestReconnectResubscribesWithCursor(t *testing.T) {
	edge := newFakeEdge(t)
	edge.resumedValue = true
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	got := make(chan *Message, 4)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", name: "x", messageID: "m1", clientID: "c", seq: 41})
	waitFor(t, "cursor baseline", func() bool { return channel.cursor() == 41 })

	updates := make(chan ChannelStateChange, 8)
	channel.On(func(change ChannelStateChange) { updates <- change })
	edge.dropAll()
	sub := nextFrame[*subscribeFrame](t, edge)
	if sub.lastSerial != 41 {
		t.Errorf("re-sub lastSerial = %d, want 41", sub.lastSerial)
	}
	waitFor(t, "channel re-attached", func() bool { return channel.State() == ChannelAttached })
	// The suspended -> attaching -> attached walk must be visible, with the final
	// attach carrying the server's resume outcome.
	var walk []ChannelStateChange
	waitFor(t, "state walk", func() bool {
		for {
			select {
			case change := <-updates:
				walk = append(walk, change)
			default:
				return len(walk) >= 3
			}
		}
	})
	if walk[0].Current != ChannelSuspended || walk[1].Current != ChannelAttaching || walk[2].Current != ChannelAttached {
		t.Fatalf("walk = %+v", walk)
	}
	if !walk[2].Resumed {
		t.Error("final attach did not carry resumed=true")
	}
}

func TestDiscontinuityResetsCursor(t *testing.T) {
	edge := newFakeEdge(t)
	edge.resumedValue = false
	client := newTestClient(t, edge)
	channel := client.Channels.Get("room:1")
	channel.Subscribe(func(*Message) {})
	nextFrame[*subscribeFrame](t, edge)
	edge.pushLatest(&msgFrame{channel: "room:1", name: "x", messageID: "m1", clientID: "c", seq: 41})
	waitFor(t, "cursor baseline", func() bool { return channel.cursor() == 41 })
	edge.dropAll()
	nextFrame[*subscribeFrame](t, edge)
	waitFor(t, "re-attach", func() bool { return channel.State() == ChannelAttached })
	// The cursor aged out of retention (resumed=false), so it must reset: otherwise
	// every later message would look gapped and the channel would backfill-loop.
	waitFor(t, "cursor reset", func() bool { return channel.cursor() == 0 })
}

func TestHistoryExpandsAndPages(t *testing.T) {
	edge := newFakeEdge(t)
	edge.onHistory = func(hist *historyFrame) *historyResponseFrame {
		if hist.before != 10 || hist.limit != 3 {
			t.Errorf("hist = %+v", hist)
		}
		return &historyResponseFrame{
			channel: hist.channel,
			more:    true,
			messages: []msgFrame{
				{channel: hist.channel, name: "solo", data: json.RawMessage(`1`), messageID: "m1", seq: 7},
				{channel: hist.channel, messageID: "bat", seq: 8, members: []wireMember{
					{name: "a", data: json.RawMessage(`2`)},
					{name: "b", data: json.RawMessage(`3`)},
				}},
			},
		}
	}
	client := newTestClient(t, edge)
	result, err := client.Channels.Get("room:1").History(context.Background(), HistoryParams{Limit: 3, Before: 10})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if !result.More {
		t.Error("More = false")
	}
	if len(result.Messages) != 3 {
		t.Fatalf("messages = %d, want 3 (batch expanded)", len(result.Messages))
	}
	if result.Messages[1].ID != "bat:0" || result.Messages[2].ID != "bat:1" {
		t.Errorf("expanded ids = %q, %q", result.Messages[1].ID, result.Messages[2].ID)
	}
}

func TestEncryptedChannelRoundTrip(t *testing.T) {
	edge := newFakeEdge(t)
	key, err := GenerateRandomKey(256)
	if err != nil {
		t.Fatalf("GenerateRandomKey: %v", err)
	}
	cipher, err := NewCipher(CipherParams{KeyBase64: key})
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	client := newTestClient(t, edge)
	channel := client.Channels.Get("secure:1", WithCipher(cipher))
	got := make(chan *Message, 1)
	channel.Subscribe(func(message *Message) { got <- message })
	nextFrame[*subscribeFrame](t, edge)

	// Outbound: the wire payload must be ciphertext with the cipher encoding.
	if err := channel.Publish(context.Background(), "secret", map[string]string{"pin": "1234"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	publish := nextFrame[*publishFrame](t, edge)
	member := publish.members[0]
	if member.encoding != "cipher+aes-256-gcm/base64" {
		t.Errorf("encoding = %q", member.encoding)
	}
	if string(member.data) == `{"pin":"1234"}` {
		t.Error("payload left the client in plaintext")
	}

	// Inbound: a frame carrying that ciphertext decrypts back to the plaintext value.
	edge.pushLatest(&msgFrame{
		channel: "secure:1", name: "secret",
		data: member.data, encoding: member.encoding,
		messageID: "m1", clientID: "c", seq: 1,
	})
	select {
	case message := <-got:
		if message.Encoding != "" {
			t.Errorf("delivered encoding = %q, want stripped", message.Encoding)
		}
		if string(message.Data) != `{"pin":"1234"}` {
			t.Errorf("decrypted data = %s", message.Data)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("encrypted message never delivered")
	}
}

func TestFlushSendsBufferedBatch(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	if err := client.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	// A long interval would hold the second publish. Flush must force it out.
	channel := client.Channels.Get("room:1", WithBatchOptions(BatchOptions{Interval: time.Hour}))
	if err := channel.Publish(context.Background(), "first", 1); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	nextFrame[*publishFrame](t, edge)
	done := make(chan error, 1)
	go func() { done <- channel.Publish(context.Background(), "second", 2) }()
	waitFor(t, "buffered publish", func() bool {
		channel.mu.Lock()
		defer channel.mu.Unlock()
		return len(channel.batchBuf) == 1
	})
	channel.Flush()
	publish := nextFrame[*publishFrame](t, edge)
	if len(publish.members) != 1 || publish.members[0].name != "second" {
		t.Errorf("flushed members = %+v", publish.members)
	}
	if err := <-done; err != nil {
		t.Fatalf("second publish: %v", err)
	}
}
