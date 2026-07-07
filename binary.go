package realtime

// Binary opcode protocol codec — the client half of the edge's wire format. Every frame
// is a 1-byte opcode then its fields (uvarints and length-prefixed byte slices). Field
// orders mirror the server's wire package exactly; the golden tests in binary_test.go
// pin them cross-language against the same byte vectors the realtime-js SDK uses.
//
// The client only needs encodeClientFrame (frames it sends) and decodeServerFrames
// (frames it receives). The reverse direction (encodeServerFrame / decodeClientRecord)
// exists for the in-process fake edge in the tests.

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Frame opcodes — one per frame type, matching the server's wire.Op table.
const (
	opAuth      = 1
	opSub       = 2
	opUnsub     = 3
	opPub       = 4
	opPres      = 5
	opPresSub   = 6
	opPresUnsub = 7
	opHist      = 8
	opFetch     = 9
	opPing      = 10
	opConnected = 11
	opAck       = 12
	opMsg       = 13
	opPresEvt   = 14
	opErr       = 15
	opPong      = 16
	opHistRes   = 17
	opFetchRes  = 18
	opBatch     = 19
)

// flagEphemeral marks a publish or message as fire-and-forget in its flags byte.
const flagEphemeral = 1 << 0

// flagBatch, in a bundle member's flags byte, means the member is itself a batch (the
// batch layout follows instead of the single-message layout).
const flagBatch = 1 << 1

// flagResponseSet carries a history response's "more" / a fetch response's "resumed".
const flagResponseSet = 1 << 0

// authProtocolVersion is the byte after the auth opcode. A binary connection always
// coalesces and receives binary delivery (both implied by speaking binary), so instead
// of flags it carries a protocol version — 0 today, reserved for future connection-wide
// format changes.
const authProtocolVersion = 0

var errTruncated = errors.New("realtime: binary frame truncated")

// ---- write helpers ----

func appendUvarint(dst []byte, value uint64) []byte {
	return binary.AppendUvarint(dst, value)
}

func appendBytes(dst, field []byte) []byte {
	dst = appendUvarint(dst, uint64(len(field)))
	return append(dst, field...)
}

func appendString(dst []byte, value string) []byte {
	dst = appendUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func presenceActionByte(action PresenceAction) byte {
	switch action {
	case PresenceEnter:
		return 1
	case PresenceLeave:
		return 2
	case PresenceUpdate:
		return 3
	}
	return 0
}

func presenceActionFrom(value byte) PresenceAction {
	switch value {
	case 2:
		return PresenceLeave
	case 3:
		return PresenceUpdate
	}
	return PresenceEnter
}

// ---- read helper ----

// binReader is a forward cursor over a byte buffer. It records the first error and turns
// every later read into a no-op, so a decoder can read each field and check err once at
// the end instead of after every field.
type binReader struct {
	buf []byte
	err error
}

func (r *binReader) done() bool {
	return len(r.buf) == 0 || r.err != nil
}

func (r *binReader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.buf) == 0 {
		r.err = errTruncated
		return 0
	}
	value := r.buf[0]
	r.buf = r.buf[1:]
	return value
}

func (r *binReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	value, read := binary.Uvarint(r.buf)
	if read <= 0 {
		r.err = errTruncated
		return 0
	}
	r.buf = r.buf[read:]
	return value
}

// slice reads a length-prefixed field. The result aliases the buffer, so copy it before
// the buffer is reused (str and jsonField do).
func (r *binReader) slice() []byte {
	length := r.uvarint()
	if r.err != nil {
		return nil
	}
	if length > uint64(len(r.buf)) {
		r.err = errTruncated
		return nil
	}
	field := r.buf[:length]
	r.buf = r.buf[length:]
	return field
}

func (r *binReader) str() string {
	return string(r.slice())
}

// jsonField reads a length-prefixed raw JSON payload, copying it so the result outlives
// the read buffer. Empty bytes decode to nil (an absent payload).
func (r *binReader) jsonField() []byte {
	field := r.slice()
	if len(field) == 0 {
		return nil
	}
	return append([]byte(nil), field...)
}

// ---- record framing ----

// frameBinaryRecord prefixes a record with its uvarint length so several records
// concatenate in one WebSocket message (and the server splits them the same way). The
// SDK sends one frame per message.
func frameBinaryRecord(record []byte) []byte {
	framed := appendUvarint(make([]byte, 0, len(record)+2), uint64(len(record)))
	return append(framed, record...)
}

// splitBinaryRecords splits a WebSocket binary message into its length-prefixed records.
// A malformed tail yields the records recovered before it.
func splitBinaryRecords(body []byte) [][]byte {
	reader := &binReader{buf: body}
	var records [][]byte
	for !reader.done() {
		record := reader.slice()
		if reader.err != nil {
			break
		}
		records = append(records, record)
	}
	return records
}

// ---- client frames: encode ----

// encodeClientFrame encodes any client frame to its binary record (opcode + fields).
func encodeClientFrame(frame any) []byte {
	switch f := frame.(type) {
	case *authFrame:
		out := []byte{opAuth, authProtocolVersion}
		out = appendString(out, f.token)
		out = appendString(out, f.key)
		out = appendString(out, f.clientID)
		return appendString(out, f.resumeConnectionID)
	case *subscribeFrame:
		out := []byte{opSub}
		out = appendUvarint(out, f.id)
		out = appendUvarint(out, f.lastSerial)
		return appendString(out, f.channel)
	case *unsubscribeFrame:
		return encodeChanFrame(opUnsub, f.id, f.channel)
	case *publishFrame:
		return encodeBinaryPublish(f)
	case *presenceFrame:
		out := []byte{opPres, presenceActionByte(f.action)}
		out = appendUvarint(out, f.id)
		out = appendString(out, f.channel)
		out = appendBytes(out, f.data)
		return appendString(out, f.encoding)
	case *presenceSubscribeFrame:
		return encodeChanFrame(opPresSub, f.id, f.channel)
	case *presenceUnsubscribeFrame:
		return encodeChanFrame(opPresUnsub, f.id, f.channel)
	case *historyFrame:
		out := []byte{opHist}
		out = appendUvarint(out, f.id)
		out = appendUvarint(out, f.limit)
		out = appendString(out, f.channel)
		return appendUvarint(out, f.before)
	case *fetchFrame:
		out := []byte{opFetch}
		out = appendUvarint(out, f.id)
		out = appendUvarint(out, f.fromSerial)
		return appendString(out, f.channel)
	case *pingFrame:
		return []byte{opPing}
	}
	panic(fmt.Sprintf("realtime: encodeClientFrame: unhandled frame %T", frame))
}

func encodeChanFrame(op byte, id uint64, channel string) []byte {
	out := []byte{op}
	out = appendUvarint(out, id)
	return appendString(out, channel)
}

// encodeBinaryPublish encodes a publish (single or batch): opcode, flags, uvarint id,
// length-prefixed channel/name/data/encoding/messageId, a reserved uvarint, then a
// uvarint member count and that many members (name/data/encoding each).
func encodeBinaryPublish(frame *publishFrame) []byte {
	var flags byte
	if frame.ephemeral {
		flags |= flagEphemeral
	}
	out := []byte{opPub, flags}
	out = appendUvarint(out, frame.id)
	out = appendString(out, frame.channel)
	out = appendString(out, frame.name)
	out = appendBytes(out, frame.data)
	out = appendString(out, frame.encoding)
	out = appendString(out, frame.messageID)
	// Reserved slot: was the per-message ttlMs, dead since the retention-tier rework.
	// Kept as a zero on purpose: removing it would break or silently corrupt publishes
	// from older SDKs, and a zero-meaning-absent uvarint is a free extension point for
	// a future optional field. Do not remove without a publish tag bump.
	out = appendUvarint(out, 0)
	out = appendUvarint(out, uint64(len(frame.members)))
	for i := range frame.members {
		member := &frame.members[i]
		out = appendString(out, member.name)
		out = appendBytes(out, member.data)
		out = appendString(out, member.encoding)
	}
	return out
}

// ---- shared message members (msg delivery and history/fetch responses) ----

// readMember reads one message member: its flags byte, then either the single-message
// layout or (flagBatch) the batch layout.
func readMember(reader *binReader) msgFrame {
	flags := reader.byte()
	if flags&flagBatch != 0 {
		return readBatchFields(reader, flags)
	}
	var frame msgFrame
	frame.timestamp = reader.uvarint()
	frame.channel = reader.str()
	frame.name = reader.str()
	frame.data = reader.jsonField()
	frame.messageID = reader.str()
	frame.clientID = reader.str()
	frame.encoding = reader.str()
	frame.seq = reader.uvarint()
	frame.ephemeral = flags&flagEphemeral != 0
	return frame
}

// readBatchFields reads a batch's fields (after its flags byte): the shared header then
// each payload's name/data/encoding.
func readBatchFields(reader *binReader, flags byte) msgFrame {
	var frame msgFrame
	frame.timestamp = reader.uvarint()
	frame.channel = reader.str()
	frame.messageID = reader.str()
	frame.clientID = reader.str()
	frame.seq = reader.uvarint()
	frame.ephemeral = flags&flagEphemeral != 0
	count := reader.uvarint()
	// Bound the allocation by the record's actual size: the smallest member is three
	// 1-byte length prefixes, so a forged count cannot drive a huge allocation.
	if count > uint64(len(reader.buf))/3 {
		reader.err = errTruncated
		return frame
	}
	frame.members = make([]wireMember, 0, count)
	for i := uint64(0); i < count; i++ {
		var member wireMember
		member.name = reader.str()
		member.data = reader.jsonField()
		member.encoding = reader.str()
		frame.members = append(frame.members, member)
	}
	return frame
}

// toBundled narrows a decoded member to its bundle shape (the channel travels on the
// carrying frame).
func toBundled(member msgFrame) bundledMessage {
	return bundledMessage{
		name:      member.name,
		data:      member.data,
		timestamp: member.timestamp,
		messageID: member.messageID,
		clientID:  member.clientID,
		encoding:  member.encoding,
		seq:       member.seq,
		ephemeral: member.ephemeral,
		members:   member.members,
	}
}

// ---- server frames: decode ----

// decodeServerFrames splits a WebSocket binary message into records and decodes each
// server frame by its opcode. A malformed record or tail yields the frames decoded
// before it.
func decodeServerFrames(body []byte) []any {
	records := splitBinaryRecords(body)
	frames := make([]any, 0, len(records))
	for _, record := range records {
		if frame := decodeServerFrame(record); frame != nil {
			frames = append(frames, frame)
		}
	}
	return frames
}

// decodeServerFrame decodes one server record, or nil for an unknown opcode or a
// malformed record.
func decodeServerFrame(record []byte) any {
	if len(record) == 0 {
		return nil
	}
	reader := &binReader{buf: record[1:]}
	frame := decodeServerFrameBody(record[0], reader)
	if reader.err != nil {
		return nil
	}
	return frame
}

func decodeServerFrameBody(op byte, reader *binReader) any {
	switch op {
	case opConnected:
		return &connectedFrame{connectionID: reader.str(), keepAliveMs: reader.uvarint(), clientID: reader.str()}
	case opAck:
		flags := reader.byte()
		frame := &ackFrame{}
		frame.id = reader.uvarint()
		frame.seq = reader.uvarint()
		if flags&1 != 0 {
			resumed := flags&2 != 0
			frame.resumed = &resumed
		}
		return frame
	case opMsg:
		return decodeMessage(reader)
	case opBatch:
		flags := reader.byte()
		frame := readBatchFields(reader, flags)
		return &frame
	case opPresEvt:
		action := presenceActionFrom(reader.byte())
		return &presenceEventFrame{
			action:       action,
			timestamp:    reader.uvarint(),
			channel:      reader.str(),
			clientID:     reader.str(),
			connectionID: reader.str(),
			data:         reader.jsonField(),
			encoding:     reader.str(),
		}
	case opErr:
		return &errorFrame{id: reader.uvarint(), code: int(reader.uvarint()), message: reader.str()}
	case opPong:
		return &pongFrame{}
	case opHistRes:
		id, channel, messages, flag := decodeResponse(reader)
		return &historyResponseFrame{id: id, channel: channel, messages: messages, more: flag}
	case opFetchRes:
		id, channel, messages, flag := decodeResponse(reader)
		return &fetchResponseFrame{id: id, channel: channel, messages: messages, resumed: flag}
	}
	return nil
}

// decodeMessage reads an opMsg record's members: one is a single message, several a
// server-coalesced bundle.
func decodeMessage(reader *binReader) *msgFrame {
	count := reader.uvarint()
	if count == 1 {
		frame := readMember(reader)
		return &frame
	}
	// The smallest member is a flags byte plus eight 1-byte fields.
	if count > uint64(len(reader.buf))/9+1 {
		reader.err = errTruncated
		return nil
	}
	frame := &msgFrame{}
	frame.bundle = make([]bundledMessage, 0, count)
	for i := uint64(0); i < count; i++ {
		member := readMember(reader)
		frame.channel = member.channel
		frame.bundle = append(frame.bundle, toBundled(member))
	}
	return frame
}

func decodeResponse(reader *binReader) (id uint64, channel string, messages []msgFrame, flag bool) {
	flag = reader.byte()&flagResponseSet != 0
	id = reader.uvarint()
	channel = reader.str()
	count := reader.uvarint()
	if count > uint64(len(reader.buf))/9+1 {
		reader.err = errTruncated
		return id, channel, nil, flag
	}
	messages = make([]msgFrame, 0, count)
	for i := uint64(0); i < count; i++ {
		messages = append(messages, readMember(reader))
	}
	return id, channel, messages, flag
}

// ---- reverse direction (the in-process fake edge in tests only) ----

// pushMember writes one message member: flags, then the single or batch layout.
func pushMember(out []byte, frame *msgFrame, channel string) []byte {
	isBatch := len(frame.members) > 0
	var flags byte
	if frame.ephemeral {
		flags |= flagEphemeral
	}
	if isBatch {
		flags |= flagBatch
	}
	out = append(out, flags)
	if isBatch {
		return pushBatchFields(out, frame, channel)
	}
	out = appendUvarint(out, frame.timestamp)
	out = appendString(out, channel)
	out = appendString(out, frame.name)
	out = appendBytes(out, frame.data)
	out = appendString(out, frame.messageID)
	out = appendString(out, frame.clientID)
	out = appendString(out, frame.encoding)
	return appendUvarint(out, frame.seq)
}

// pushBatchFields writes a batch's fields after its flags byte: the shared header then
// each payload's name/data/encoding.
func pushBatchFields(out []byte, frame *msgFrame, channel string) []byte {
	out = appendUvarint(out, frame.timestamp)
	out = appendString(out, channel)
	out = appendString(out, frame.messageID)
	out = appendString(out, frame.clientID)
	out = appendUvarint(out, frame.seq)
	out = appendUvarint(out, uint64(len(frame.members)))
	for i := range frame.members {
		member := &frame.members[i]
		out = appendString(out, member.name)
		out = appendBytes(out, member.data)
		out = appendString(out, member.encoding)
	}
	return out
}

// bundledToMsg widens a bundle member back to a msgFrame for encoding.
func bundledToMsg(channel string, member bundledMessage) msgFrame {
	return msgFrame{
		channel:   channel,
		name:      member.name,
		data:      member.data,
		timestamp: member.timestamp,
		messageID: member.messageID,
		clientID:  member.clientID,
		encoding:  member.encoding,
		seq:       member.seq,
		ephemeral: member.ephemeral,
		members:   member.members,
	}
}

// encodeServerFrame encodes a server frame for the test fake edge.
func encodeServerFrame(frame any) []byte {
	switch f := frame.(type) {
	case *connectedFrame:
		out := []byte{opConnected}
		out = appendString(out, f.connectionID)
		out = appendUvarint(out, f.keepAliveMs)
		return appendString(out, f.clientID)
	case *ackFrame:
		var flags byte
		if f.resumed != nil {
			flags = 1
			if *f.resumed {
				flags |= 2
			}
		}
		out := []byte{opAck, flags}
		out = appendUvarint(out, f.id)
		return appendUvarint(out, f.seq)
	case *msgFrame:
		return encodeMessage(f)
	case *presenceEventFrame:
		out := []byte{opPresEvt, presenceActionByte(f.action)}
		out = appendUvarint(out, f.timestamp)
		out = appendString(out, f.channel)
		out = appendString(out, f.clientID)
		out = appendString(out, f.connectionID)
		out = appendBytes(out, f.data)
		return appendString(out, f.encoding)
	case *errorFrame:
		out := []byte{opErr}
		out = appendUvarint(out, f.id)
		out = appendUvarint(out, uint64(f.code))
		return appendString(out, f.message)
	case *pongFrame:
		return []byte{opPong}
	case *historyResponseFrame:
		return encodeResponse(opHistRes, f.id, f.channel, f.messages, f.more)
	case *fetchResponseFrame:
		return encodeResponse(opFetchRes, f.id, f.channel, f.messages, f.resumed)
	}
	panic(fmt.Sprintf("realtime: encodeServerFrame: unhandled frame %T", frame))
}

// encodeMessage encodes a msgFrame: a batch as an opBatch record, otherwise an opMsg
// record carrying one member or the bundle's members.
func encodeMessage(frame *msgFrame) []byte {
	if len(frame.members) > 0 {
		var flags byte
		if frame.ephemeral {
			flags |= flagEphemeral
		}
		out := []byte{opBatch, flags}
		return pushBatchFields(out, frame, frame.channel)
	}
	out := []byte{opMsg}
	if len(frame.bundle) > 0 {
		out = appendUvarint(out, uint64(len(frame.bundle)))
		for _, member := range frame.bundle {
			widened := bundledToMsg(frame.channel, member)
			out = pushMember(out, &widened, frame.channel)
		}
		return out
	}
	out = appendUvarint(out, 1)
	return pushMember(out, frame, frame.channel)
}

func encodeResponse(op byte, id uint64, channel string, messages []msgFrame, flag bool) []byte {
	var flags byte
	if flag {
		flags = flagResponseSet
	}
	out := []byte{op, flags}
	out = appendUvarint(out, id)
	out = appendString(out, channel)
	out = appendUvarint(out, uint64(len(messages)))
	for i := range messages {
		out = pushMember(out, &messages[i], channel)
	}
	return out
}

// decodeClientRecord decodes a client frame for the test fake edge.
func decodeClientRecord(record []byte) (any, error) {
	if len(record) == 0 {
		return nil, errTruncated
	}
	reader := &binReader{buf: record[1:]}
	frame := decodeClientRecordBody(record[0], reader)
	if frame == nil {
		return nil, fmt.Errorf("realtime: decodeClientRecord: unknown opcode %d", record[0])
	}
	if reader.err != nil {
		return nil, reader.err
	}
	return frame, nil
}

func decodeClientRecordBody(op byte, reader *binReader) any {
	switch op {
	case opAuth:
		reader.byte() // protocol version (authProtocolVersion); 0 today, reserved
		return &authFrame{token: reader.str(), key: reader.str(), clientID: reader.str(), resumeConnectionID: reader.str()}
	case opSub:
		return &subscribeFrame{id: reader.uvarint(), lastSerial: reader.uvarint(), channel: reader.str()}
	case opUnsub:
		return &unsubscribeFrame{id: reader.uvarint(), channel: reader.str()}
	case opPub:
		return decodeClientPublish(reader)
	case opPres:
		action := presenceActionFrom(reader.byte())
		return &presenceFrame{action: action, id: reader.uvarint(), channel: reader.str(), data: reader.jsonField(), encoding: reader.str()}
	case opPresSub:
		return &presenceSubscribeFrame{id: reader.uvarint(), channel: reader.str()}
	case opPresUnsub:
		return &presenceUnsubscribeFrame{id: reader.uvarint(), channel: reader.str()}
	case opHist:
		return &historyFrame{id: reader.uvarint(), limit: reader.uvarint(), channel: reader.str(), before: reader.uvarint()}
	case opFetch:
		return &fetchFrame{id: reader.uvarint(), fromSerial: reader.uvarint(), channel: reader.str()}
	case opPing:
		return &pingFrame{}
	}
	return nil
}

func decodeClientPublish(reader *binReader) *publishFrame {
	flags := reader.byte()
	frame := &publishFrame{}
	frame.id = reader.uvarint()
	frame.channel = reader.str()
	frame.name = reader.str()
	frame.data = reader.jsonField()
	frame.encoding = reader.str()
	frame.messageID = reader.str()
	// Reserved slot: was the per-message ttlMs. Read and discarded; see
	// encodeBinaryPublish for why it stays on the wire.
	reader.uvarint()
	count := reader.uvarint()
	if count > uint64(len(reader.buf))/3 {
		reader.err = errTruncated
		return frame
	}
	frame.members = make([]wireMember, 0, count)
	for i := uint64(0); i < count; i++ {
		var member wireMember
		member.name = reader.str()
		member.data = reader.jsonField()
		member.encoding = reader.str()
		frame.members = append(frame.members, member)
	}
	frame.ephemeral = flags&flagEphemeral != 0
	return frame
}
