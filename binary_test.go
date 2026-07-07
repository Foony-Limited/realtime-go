package realtime

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

// mustHex decodes a hex golden vector.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	out, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex: %v", err)
	}
	return out
}

// TestGoldenPublishEncoding pins the publish encoder to the same golden bytes the
// realtime-js SDK and the server use, so the three codecs cannot drift apart.
func TestGoldenPublishEncoding(t *testing.T) {
	single := &publishFrame{
		channel: "room:1", name: "chat", data: json.RawMessage(`{"hi":"there"}`),
		ephemeral: true, messageID: "m-1", id: 7,
	}
	want := "04010706726f6f6d3a3104636861740e7b226869223a227468657265227d00036d2d310000"
	if got := hex.EncodeToString(encodeClientFrame(single)); got != want {
		t.Errorf("single publish\n got  %s\n want %s", got, want)
	}

	batch := &publishFrame{
		channel: "room:1", data: json.RawMessage(`null`), messageID: "m-2", id: 9,
		members: []wireMember{
			{name: "a", data: json.RawMessage(`{"i":1}`)},
			{name: "b", data: json.RawMessage(`"two"`), encoding: "enc"},
		},
	}
	want = "04000906726f6f6d3a3100046e756c6c00036d2d3200020161077b2269223a317d000162052274776f2203656e63"
	if got := hex.EncodeToString(encodeClientFrame(batch)); got != want {
		t.Errorf("batch publish\n got  %s\n want %s", got, want)
	}
}

// goldenMsg is a single message record (opcode 0x0d, count 1) as the server encodes it,
// shared with the realtime-js golden tests.
const goldenMsg = "540d0101f0d6a183f23306726f6f6d3a310463686174157b226869223a2274686572" +
	"65222c226e223a34327d1c313738323935353134323030303030303030302d316132623363346408636c69656e742d3700b960"

// TestGoldenMessageDecoding pins the message decoder to the server's encoding.
func TestGoldenMessageDecoding(t *testing.T) {
	frames := decodeServerFrames(mustHex(t, goldenMsg))
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	frame, ok := frames[0].(*msgFrame)
	if !ok {
		t.Fatalf("frame type %T, want *msgFrame", frames[0])
	}
	if frame.channel != "room:1" || frame.name != "chat" {
		t.Errorf("channel/name = %q/%q", frame.channel, frame.name)
	}
	if string(frame.data) != `{"hi":"there","n":42}` {
		t.Errorf("data = %s", frame.data)
	}
	if frame.timestamp != 1782955142000 {
		t.Errorf("timestamp = %d", frame.timestamp)
	}
	if frame.messageID != "1782955142000000000-1a2b3c4d" || frame.clientID != "client-7" {
		t.Errorf("messageID/clientID = %q/%q", frame.messageID, frame.clientID)
	}
	if frame.seq != 12345 || !frame.ephemeral {
		t.Errorf("seq/ephemeral = %d/%v", frame.seq, frame.ephemeral)
	}
}

// TestGoldenCoalescedRecords checks several records in one WebSocket message all decode.
func TestGoldenCoalescedRecords(t *testing.T) {
	frames := decodeServerFrames(mustHex(t, goldenMsg+goldenMsg))
	if len(frames) != 2 {
		t.Fatalf("frames = %d, want 2", len(frames))
	}
	if frames[1].(*msgFrame).messageID != "1782955142000000000-1a2b3c4d" {
		t.Error("second record decoded wrong")
	}
}

// TestGoldenMalformedTail checks whole frames decoded before a malformed tail survive.
func TestGoldenMalformedTail(t *testing.T) {
	frames := decodeServerFrames(mustHex(t, goldenMsg+"ff"))
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
}

// TestGoldenBundleDecoding pins the bundle decoder (opcode 0x0d, count 2).
func TestGoldenBundleDecoding(t *testing.T) {
	golden := "3a0d02006406726f6f6d3a310161077b2269223a317d0469642d310263310005" +
		"016506726f6f6d3a310162052274776f220469642d320263320006"
	frames := decodeServerFrames(mustHex(t, golden))
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	frame := frames[0].(*msgFrame)
	if frame.channel != "room:1" || len(frame.bundle) != 2 {
		t.Fatalf("channel %q bundle %d", frame.channel, len(frame.bundle))
	}
	first, second := frame.bundle[0], frame.bundle[1]
	if first.name != "a" || string(first.data) != `{"i":1}` || first.messageID != "id-1" || first.clientID != "c1" || first.seq != 5 {
		t.Errorf("first member = %+v", first)
	}
	if second.name != "b" || string(second.data) != `"two"` || second.messageID != "id-2" || second.clientID != "c2" || second.seq != 6 || !second.ephemeral {
		t.Errorf("second member = %+v", second)
	}
}

// TestGoldenBatchDecoding pins the batch decoder (opcode 0x13).
func TestGoldenBatchDecoding(t *testing.T) {
	golden := "361301f0d6a183f23306726f6f6d3a310362617408636c69656e742d37b960020161" +
		"077b2269223a317d000162052274776f2203656e63"
	frames := decodeServerFrames(mustHex(t, golden))
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	frame := frames[0].(*msgFrame)
	if frame.channel != "room:1" || frame.messageID != "bat" || frame.clientID != "client-7" {
		t.Errorf("header = %+v", frame)
	}
	if frame.timestamp != 1782955142000 || frame.seq != 12345 || !frame.ephemeral {
		t.Errorf("timestamp/seq/ephemeral = %d/%d/%v", frame.timestamp, frame.seq, frame.ephemeral)
	}
	want := []wireMember{
		{name: "a", data: json.RawMessage(`{"i":1}`)},
		{name: "b", data: json.RawMessage(`"two"`), encoding: "enc"},
	}
	if !reflect.DeepEqual(frame.members, want) {
		t.Errorf("members = %+v", frame.members)
	}
}

// TestGoldenBundleWithBatchMember pins a bundle whose second member is itself a batch
// (flags 0x02).
func TestGoldenBundleWithBatchMember(t *testing.T) {
	golden := "420d02000006726f6f6d3a310161077b2269223a317d0469642d31026331000502" +
		"6406726f6f6d3a3103626174026332060201780131000179052274776f2203656e63"
	frames := decodeServerFrames(mustHex(t, golden))
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	frame := frames[0].(*msgFrame)
	if len(frame.bundle) != 2 {
		t.Fatalf("bundle = %d, want 2", len(frame.bundle))
	}
	batch := frame.bundle[1]
	if batch.messageID != "bat" || batch.clientID != "c2" || batch.seq != 6 {
		t.Errorf("batch member header = %+v", batch)
	}
	want := []wireMember{
		{name: "x", data: json.RawMessage(`1`)},
		{name: "y", data: json.RawMessage(`"two"`), encoding: "enc"},
	}
	if !reflect.DeepEqual(batch.members, want) {
		t.Errorf("batch members = %+v", batch.members)
	}
}

// TestClientFrameRoundTrip encodes every client frame, decodes it with the fake edge's
// decoder, and re-encodes — the bytes must match, proving both directions agree.
func TestClientFrameRoundTrip(t *testing.T) {
	frames := []any{
		&authFrame{token: "tok", clientID: "c", resumeConnectionID: "r"},
		&authFrame{key: "app.key:secret"},
		&subscribeFrame{channel: "ch", id: 1, lastSerial: 9},
		&unsubscribeFrame{channel: "ch", id: 2},
		&publishFrame{channel: "ch", name: "n", data: json.RawMessage(`{"x":1}`), encoding: "e", messageID: "mid", id: 3, ephemeral: true},
		&publishFrame{channel: "ch", messageID: "bat", id: 4, members: []wireMember{
			{name: "a", data: json.RawMessage(`1`)},
			{name: "b", data: json.RawMessage(`"two"`), encoding: "enc"},
		}},
		&presenceFrame{channel: "ch", action: PresenceEnter, data: json.RawMessage(`{"a":1}`), encoding: "e", id: 5},
		&presenceFrame{channel: "ch", action: PresenceLeave, id: 6},
		&presenceSubscribeFrame{channel: "ch", id: 7},
		&presenceUnsubscribeFrame{channel: "ch", id: 8},
		&historyFrame{channel: "ch", limit: 50, before: 12, id: 9},
		&fetchFrame{channel: "ch", fromSerial: 11, id: 10},
		&pingFrame{},
	}
	for _, original := range frames {
		encoded := encodeClientFrame(original)
		decoded, err := decodeClientRecord(encoded)
		if err != nil {
			t.Fatalf("decodeClientRecord %T: %v", original, err)
		}
		if reencoded := encodeClientFrame(decoded); !bytes.Equal(encoded, reencoded) {
			t.Errorf("%T round-trip mismatch\n first  %x\n second %x", original, encoded, reencoded)
		}
	}
}

// TestServerFrameRoundTrip does the same for server frames via the fake edge's encoder.
func TestServerFrameRoundTrip(t *testing.T) {
	resumed := true
	single := msgFrame{channel: "ch", name: "a", data: json.RawMessage(`1`), messageID: "m1", clientID: "c", seq: 1, timestamp: 42}
	frames := []any{
		&connectedFrame{connectionID: "conn", keepAliveMs: 30000, clientID: "c"},
		&ackFrame{id: 3, seq: 5, resumed: &resumed},
		&ackFrame{id: 4},
		&single,
		&msgFrame{channel: "ch", bundle: []bundledMessage{
			{name: "a", data: json.RawMessage(`1`), messageID: "m1", clientID: "c", seq: 1, timestamp: 9},
			{name: "b", data: json.RawMessage(`2`), messageID: "m2", clientID: "c", seq: 2, timestamp: 9, ephemeral: true},
		}},
		&msgFrame{channel: "ch", timestamp: 42, messageID: "bat", clientID: "c", seq: 7, ephemeral: true, members: []wireMember{
			{name: "a", data: json.RawMessage(`{"i":1}`)},
			{name: "b", data: json.RawMessage(`"two"`), encoding: "enc"},
		}},
		&presenceEventFrame{channel: "ch", action: PresenceLeave, clientID: "c", connectionID: "conn", data: json.RawMessage(`{}`), encoding: "e", timestamp: 9},
		&errorFrame{id: 3, code: 40001, message: "bad frame"},
		&pongFrame{},
		&historyResponseFrame{id: 7, channel: "ch", messages: []msgFrame{single}, more: true},
		&fetchResponseFrame{id: 8, channel: "ch", messages: []msgFrame{single}, resumed: true},
	}
	for _, original := range frames {
		encoded := encodeServerFrame(original)
		decoded := decodeServerFrame(encoded)
		if decoded == nil {
			t.Fatalf("decodeServerFrame %T: nil", original)
		}
		if reencoded := encodeServerFrame(decoded); !bytes.Equal(encoded, reencoded) {
			t.Errorf("%T round-trip mismatch\n first  %x\n second %x", original, encoded, reencoded)
		}
	}
}

// TestForgedCountRejected checks a member count larger than the record could hold is
// rejected before any count-sized allocation.
func TestForgedCountRejected(t *testing.T) {
	record := []byte{opMsg}
	record = appendUvarint(record, uint64(1)<<62)
	if frame := decodeServerFrame(record); frame != nil {
		t.Error("forged bundle count: want nil frame")
	}
	response := []byte{opHistRes, 0}
	response = appendUvarint(response, 1)
	response = appendString(response, "ch")
	response = appendUvarint(response, uint64(1)<<62)
	if frame := decodeServerFrame(response); frame != nil {
		t.Error("forged response count: want nil frame")
	}
}

// TestChannelNameValidation mirrors the server grammar the SDK enforces client-side.
func TestChannelNameValidation(t *testing.T) {
	valid := []string{"a", "chat:rooms:42", "A-Z_0", "x"}
	for _, name := range valid {
		if !validChannelName(name) {
			t.Errorf("validChannelName(%q) = false, want true", name)
		}
	}
	invalid := []string{"", ":lead", "has.dot", "has space", "é", string(make([]byte, 256))}
	for _, name := range invalid {
		if validChannelName(name) {
			t.Errorf("validChannelName(%q) = true, want false", name)
		}
	}
}
