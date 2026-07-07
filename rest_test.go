package realtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// restServer starts an httptest server and returns it plus a Rest client pointed at it.
func restServer(t *testing.T, handler http.HandlerFunc, mutate ...func(*RestOptions)) (*httptest.Server, *Rest) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	options := RestOptions{Endpoint: server.URL, Key: "app.main:secret"}
	for _, fn := range mutate {
		fn(&options)
	}
	rest, err := NewRest(options)
	if err != nil {
		t.Fatalf("NewRest: %v", err)
	}
	return server, rest
}

func TestNewRestRequiresAuth(t *testing.T) {
	if _, err := NewRest(RestOptions{}); err == nil {
		t.Error("NewRest with no auth: want error")
	}
}

func TestRestPublishSingle(t *testing.T) {
	var gotAuth, gotPath string
	var gotBody map[string]any
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		fmt.Fprint(w, `{"messageId":"srv-1","serial":9}`)
	})
	result, err := rest.Channels.Get("chat:room-1").Publish(context.Background(), "greeting", map[string]string{"text": "hi"})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.MessageID != "srv-1" || result.Serial != 9 {
		t.Errorf("result = %+v", result)
	}
	if gotPath != "/channels/chat:room-1/messages" {
		t.Errorf("path = %q", gotPath)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("app.main:secret"))
	if gotAuth != wantAuth {
		t.Errorf("auth = %q", gotAuth)
	}
	if gotBody["name"] != "greeting" {
		t.Errorf("body = %v", gotBody)
	}
}

func TestRestPublishBatchAndFields(t *testing.T) {
	var gotBody []map[string]any
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		fmt.Fprint(w, `{"messageId":"srv-2"}`)
	}, func(options *RestOptions) { options.ClientID = "backend-bot" })
	_, err := rest.Channels.Get("a").PublishMessages(context.Background(),
		RestPublishMessage{Name: "x", Data: 1, ID: "stable-1"},
		RestPublishMessage{Name: "y", Data: 2, ClientID: "override", Ephemeral: true},
	)
	if err != nil {
		t.Fatalf("PublishMessages: %v", err)
	}
	if len(gotBody) != 2 {
		t.Fatalf("body = %v", gotBody)
	}
	// The client default fills a missing clientId, and an explicit one wins.
	if gotBody[0]["clientId"] != "backend-bot" || gotBody[0]["id"] != "stable-1" {
		t.Errorf("first = %v", gotBody[0])
	}
	if gotBody[1]["clientId"] != "override" || gotBody[1]["ephemeral"] != true {
		t.Errorf("second = %v", gotBody[1])
	}
}

func TestRestHistoryPagination(t *testing.T) {
	var server *httptest.Server
	server, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery == "limit=2" {
			w.Header().Set("Link", "<"+"/channels/a/messages?before=8&limit=2"+">; rel=\"next\"")
			fmt.Fprint(w, `[{"id":"m9","serial":9},{"id":"m8","serial":8}]`)
			return
		}
		if r.URL.Query().Get("before") == "8" {
			fmt.Fprint(w, `[{"id":"m7","serial":7}]`)
			return
		}
		http.Error(w, "unexpected query "+r.URL.RawQuery, http.StatusBadRequest)
	})
	_ = server
	page, err := rest.Channels.Get("a").History(context.Background(), RestHistoryParams{Limit: 2})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(page.Items) != 2 || page.Items[0].ID != "m9" {
		t.Errorf("page 1 = %+v", page.Items)
	}
	if !page.HasNext() || page.IsLast() {
		t.Error("page 1 should have a next page")
	}
	next, err := page.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if len(next.Items) != 1 || next.Items[0].ID != "m7" {
		t.Errorf("page 2 = %+v", next.Items)
	}
	if next.HasNext() {
		t.Error("page 2 should be the last")
	}
	last, err := next.Next(context.Background())
	if err != nil || last != nil {
		t.Errorf("Next on the last page = %v, %v", last, err)
	}
}

func TestRestErrorParsing(t *testing.T) {
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error":{"message":"capability denied","code":40301,"statusCode":403}}`)
	})
	_, err := rest.Channels.Get("a").Publish(context.Background(), "x", 1)
	var restErr *RestError
	if !errors.As(err, &restErr) {
		t.Fatalf("error = %v", err)
	}
	if restErr.Code != CodeCapability || restErr.StatusCode != http.StatusForbidden {
		t.Errorf("restErr = %+v", restErr)
	}
}

func TestRestAuthCallbackRetriesOn401(t *testing.T) {
	var calls atomic.Int32
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer stale" {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":{"message":"expired","code":40102}}`)
			return
		}
		fmt.Fprint(w, `{"messageId":"ok"}`)
	}, func(options *RestOptions) {
		options.Key = ""
		options.AuthCallback = func(ctx context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "stale", nil
			}
			return "fresh", nil
		}
	})
	result, err := rest.Channels.Get("a").Publish(context.Background(), "x", 1)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.MessageID != "ok" || calls.Load() != 2 {
		t.Errorf("result = %+v, callback calls = %d", result, calls.Load())
	}
}

func TestRestPresenceGet(t *testing.T) {
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/channels/a/presence" || r.URL.Query().Get("clientId") != "alice" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `[{"clientId":"alice","connectionId":"conn-1","action":"present","timestamp":5}]`)
	})
	page, err := rest.Channels.Get("a").Presence.Get(context.Background(), RestPresenceParams{ClientID: "alice"})
	if err != nil {
		t.Fatalf("Presence.Get: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].ConnectionID != "conn-1" {
		t.Errorf("members = %+v", page.Items)
	}
	if page.HasNext() {
		t.Error("presence snapshots are a single page")
	}
}

func TestRestRequestToken(t *testing.T) {
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/keys/app.main/requestToken" {
			http.Error(w, "bad path "+r.URL.Path, http.StatusBadRequest)
			return
		}
		var body map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		if body["clientId"] != "user-1" || body["ttl"] != float64(60000) {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `{"token":"jwt-1","keyName":"app.main","issued":1,"expires":2,"clientId":"user-1","capability":"{}"}`)
	})
	details, err := rest.Auth.RequestToken(context.Background(), TokenParams{ClientID: "user-1", TTL: 60_000_000_000})
	if err != nil {
		t.Fatalf("RequestToken: %v", err)
	}
	if details.Token != "jwt-1" || details.Expires != 2 {
		t.Errorf("details = %+v", details)
	}
}

func TestRestTime(t *testing.T) {
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			http.Error(w, "time must not need auth", http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, `[1783000000000]`)
	})
	now, err := rest.Time(context.Background())
	if err != nil || now != 1783000000000 {
		t.Errorf("Time = %d, %v", now, err)
	}
}

func TestRestEncryptedChannel(t *testing.T) {
	cipher, err := NewCipher(CipherParams{Key: testKey32()})
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	var published map[string]any
	_, rest := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &published)
			fmt.Fprint(w, `{"messageId":"m"}`)
			return
		}
		// Echo the published ciphertext back as a history item.
		data, _ := json.Marshal(published["data"])
		fmt.Fprintf(w, `[{"id":"m","name":"secret","data":%s,"encoding":%q,"timestamp":1}]`, data, published["encoding"])
	})
	channel := rest.Channels.Get("secure", WithCipher(cipher))
	if _, err := channel.Publish(context.Background(), "secret", map[string]string{"pin": "1234"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if published["encoding"] != "cipher+aes-256-gcm/base64" {
		t.Errorf("published encoding = %v", published["encoding"])
	}
	page, err := channel.History(context.Background(), RestHistoryParams{})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(page.Items) != 1 || string(page.Items[0].Data) != `{"pin":"1234"}` || page.Items[0].Encoding != "" {
		t.Errorf("decrypted history = %+v", page.Items)
	}
}
