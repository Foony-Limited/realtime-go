package realtime

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestPresenceEnterUpdateLeave(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	presence := client.Channels.Get("room:1").Presence
	if err := presence.Enter(context.Background(), map[string]string{"status": "online"}); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	nextFrame[*subscribeFrame](t, edge) // Enter attaches first
	enter := nextFrame[*presenceFrame](t, edge)
	if enter.action != PresenceEnter || string(enter.data) != `{"status":"online"}` {
		t.Errorf("enter = %+v", enter)
	}
	if err := presence.Update(context.Background(), map[string]string{"status": "away"}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	update := nextFrame[*presenceFrame](t, edge)
	if update.action != PresenceUpdate {
		t.Errorf("update = %+v", update)
	}
	if err := presence.Leave(context.Background()); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	leave := nextFrame[*presenceFrame](t, edge)
	if leave.action != PresenceLeave || len(leave.data) != 0 {
		t.Errorf("leave = %+v", leave)
	}
}

func TestPresenceWatcherFollowsListeners(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	presence := client.Channels.Get("room:1").Presence
	// The first listener opens the server-side watcher...
	off := presence.Subscribe(func(*PresenceEvent) {})
	sub := nextFrame[*presenceSubscribeFrame](t, edge)
	if sub.channel != "room:1" {
		t.Errorf("presSub channel = %q", sub.channel)
	}
	// ...and removing the last listener closes it again.
	off()
	unsub := nextFrame[*presenceUnsubscribeFrame](t, edge)
	if unsub.channel != "room:1" {
		t.Errorf("presUnsub channel = %q", unsub.channel)
	}
}

func TestPresenceEventsDeliveredByAction(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	presence := client.Channels.Get("room:1").Presence
	enters := make(chan *PresenceEvent, 2)
	all := make(chan *PresenceEvent, 4)
	presence.On(PresenceEnter, func(event *PresenceEvent) { enters <- event })
	presence.Subscribe(func(event *PresenceEvent) { all <- event })
	nextFrame[*presenceSubscribeFrame](t, edge)
	edge.pushLatest(&presenceEventFrame{channel: "room:1", action: PresenceEnter, clientID: "alice", connectionID: "conn-9", data: json.RawMessage(`{"a":1}`), timestamp: 5})
	edge.pushLatest(&presenceEventFrame{channel: "room:1", action: PresenceLeave, clientID: "alice", connectionID: "conn-9", timestamp: 6})
	select {
	case event := <-enters:
		if event.ClientID != "alice" || event.Timestamp != 5 {
			t.Errorf("enter event = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enter never delivered")
	}
	var actions []PresenceAction
	waitFor(t, "both events on the catch-all", func() bool {
		for {
			select {
			case event := <-all:
				actions = append(actions, event.Action)
			default:
				return len(actions) == 2
			}
		}
	})
	if actions[0] != PresenceEnter || actions[1] != PresenceLeave {
		t.Errorf("actions = %v", actions)
	}
}

func TestPresenceReentersAfterReconnect(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	presence := client.Channels.Get("room:1").Presence
	if err := presence.Enter(context.Background(), "here"); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	nextFrame[*subscribeFrame](t, edge)
	nextFrame[*presenceFrame](t, edge)
	edge.dropAll()
	// The reconnect must re-subscribe the channel AND re-enter the membership.
	deadline := time.After(5 * time.Second)
	var sawSub, sawEnter bool
	for !sawSub || !sawEnter {
		select {
		case frame := <-edge.received:
			switch f := frame.(type) {
			case *subscribeFrame:
				sawSub = true
			case *presenceFrame:
				if f.action == PresenceEnter && string(f.data) == `"here"` {
					sawEnter = true
				}
			}
		case <-deadline:
			t.Fatalf("reconnect restore incomplete: sub=%v enter=%v", sawSub, sawEnter)
		}
	}
}

func TestPresenceLeaveStopsReentry(t *testing.T) {
	edge := newFakeEdge(t)
	client := newTestClient(t, edge)
	presence := client.Channels.Get("room:1").Presence
	if err := presence.Enter(context.Background(), nil); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	if err := presence.Leave(context.Background()); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	nextFrame[*subscribeFrame](t, edge)
	nextFrame[*presenceFrame](t, edge) // enter
	nextFrame[*presenceFrame](t, edge) // leave
	edge.dropAll()
	nextFrame[*subscribeFrame](t, edge) // reconnect re-sub
	select {
	case frame := <-edge.received:
		if f, ok := frame.(*presenceFrame); ok && f.action == PresenceEnter {
			t.Error("left membership was re-entered on reconnect")
		}
	case <-time.After(50 * time.Millisecond):
	}
}
