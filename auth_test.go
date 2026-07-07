package realtime

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// decodeSegment decodes one base64url JWT segment into out.
func decodeSegment(t *testing.T, segment string, out any) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("bad segment: %v", err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("bad segment JSON: %v", err)
	}
}

// TestCreateJWT checks the token structure: HS256 header with the public key name as
// kid, subject/capability/expiry claims, and a signature that verifies against the
// key's secret (the same check the edge performs).
func TestCreateJWT(t *testing.T) {
	token, err := CreateJWT("app.main:supersecret", CreateJWTParams{
		ClientID:   "user-1",
		Capability: Capability{"chat:*": {"subscribe"}},
		TTL:        time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateJWT: %v", err)
	}
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		t.Fatalf("segments = %d", len(segments))
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
		Kid string `json:"kid"`
	}
	decodeSegment(t, segments[0], &header)
	if header.Alg != "HS256" || header.Typ != "JWT" || header.Kid != "app.main" {
		t.Errorf("header = %+v", header)
	}
	var payload struct {
		Sub string `json:"sub"`
		Cap string `json:"cap"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	decodeSegment(t, segments[1], &payload)
	if payload.Sub != "user-1" {
		t.Errorf("sub = %q", payload.Sub)
	}
	// The capability travels as a JSON string claim, matching the JS SDK.
	var capability Capability
	if err := json.Unmarshal([]byte(payload.Cap), &capability); err != nil || capability["chat:*"][0] != "subscribe" {
		t.Errorf("cap = %q (err %v)", payload.Cap, err)
	}
	if payload.Exp != payload.Iat+60 {
		t.Errorf("exp - iat = %d, want 60", payload.Exp-payload.Iat)
	}
	mac := hmac.New(sha256.New, []byte("supersecret"))
	mac.Write([]byte(segments[0] + "." + segments[1]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if segments[2] != want {
		t.Error("signature does not verify against the key secret")
	}
}

// TestCreateJWTValidation checks key and param validation.
func TestCreateJWTValidation(t *testing.T) {
	params := CreateJWTParams{ClientID: "u", Capability: Capability{"*": {"subscribe"}}}
	if _, err := CreateJWT("", params); err == nil {
		t.Error("empty key: want error")
	}
	if _, err := CreateJWT("no-colon", params); err == nil {
		t.Error("colon-less key: want error")
	}
	if _, err := CreateJWT("nodot:secret", params); err == nil {
		t.Error("dot-less key name: want error")
	}
	if _, err := CreateJWT("app.k:s", CreateJWTParams{ClientID: "u"}); err == nil {
		t.Error("no capability: want error")
	}
	if _, err := CreateJWT("app.k:s", CreateJWTParams{ClientID: "u", CapabilityJSON: `{"*":["publish"]}`}); err != nil {
		t.Errorf("CapabilityJSON: %v", err)
	}
	if _, err := CreateJWT("app.k:s", CreateJWTParams{ClientID: "u", Capability: Capability{}, TTL: -time.Second}); err == nil {
		t.Error("negative TTL: want error")
	}
}

// TestClientAuthUsesConfiguredKey checks Auth.CreateJWT signs with the client's key and
// errors without one.
func TestClientAuthUsesConfiguredKey(t *testing.T) {
	withKey, err := New(Options{Key: "app.main:supersecret"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	token, err := withKey.Auth.CreateJWT(CreateJWTParams{ClientID: "u", Capability: Capability{"*": {"subscribe"}}})
	if err != nil || token == "" {
		t.Errorf("Auth.CreateJWT: %q, %v", token, err)
	}
	withToken, err := New(Options{Token: "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := withToken.Auth.CreateJWT(CreateJWTParams{ClientID: "u", Capability: Capability{}}); err == nil {
		t.Error("Auth.CreateJWT without a key: want error")
	}
}
