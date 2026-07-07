package realtime

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
)

// testKey32 is the deterministic key shared with the JS-generated golden below.
func testKey32() []byte {
	return bytes.Repeat([]byte{7}, 32)
}

// TestDecryptJSGolden decrypts a payload produced by the realtime-js SDK's Cipher with
// the same key, pinning cross-SDK compatibility of the encoding and blob layout
// (iv + ciphertext + tag, base64).
func TestDecryptJSGolden(t *testing.T) {
	cipher, err := NewCipher(CipherParams{Key: testKey32()})
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	const encoding = "cipher+aes-256-gcm/base64"
	const data = `"4kKer5hak/6G3Tbc6MO0A/C3rWvsa1zGI92jo0kggfLXB3bbuJL9pjs5ZuCvGVoj2A=="`
	plaintext, err := cipher.Decrypt(encoding, json.RawMessage(data))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(plaintext) != `{"pin":"1234","n":42}` {
		t.Errorf("plaintext = %s", plaintext)
	}
}

// TestEncryptDecryptRoundTrip checks a fresh encryption decrypts back, and that two
// encryptions of the same value differ (a fresh random IV each time).
func TestEncryptDecryptRoundTrip(t *testing.T) {
	cipher, err := NewCipher(CipherParams{Key: testKey32()})
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	value := map[string]any{"hello": "world"}
	first, err := cipher.Encrypt(value)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if first.Encoding != "cipher+aes-256-gcm/base64" {
		t.Errorf("encoding = %q", first.Encoding)
	}
	second, err := cipher.Encrypt(value)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if first.Data == second.Data {
		t.Error("two encryptions produced identical ciphertext (IV reuse)")
	}
	raw, _ := json.Marshal(first.Data)
	plaintext, err := cipher.Decrypt(first.Encoding, raw)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(plaintext) != `{"hello":"world"}` {
		t.Errorf("plaintext = %s", plaintext)
	}
}

// TestDecryptRejectsTampering checks GCM authentication fails on a flipped bit.
func TestDecryptRejectsTampering(t *testing.T) {
	cipher, err := NewCipher(CipherParams{Key: testKey32()})
	if err != nil {
		t.Fatalf("NewCipher: %v", err)
	}
	result, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	blob, _ := base64.StdEncoding.DecodeString(result.Data)
	blob[len(blob)-1] ^= 1
	tampered, _ := json.Marshal(base64.StdEncoding.EncodeToString(blob))
	if _, err := cipher.Decrypt(result.Encoding, tampered); err == nil {
		t.Error("tampered payload decrypted")
	}
}

// TestNewCipherValidation checks key length and algorithm-label rules.
func TestNewCipherValidation(t *testing.T) {
	if _, err := NewCipher(CipherParams{Key: make([]byte, 10)}); err == nil {
		t.Error("10-byte key: want error")
	}
	if _, err := NewCipher(CipherParams{}); err == nil {
		t.Error("no key: want error")
	}
	if _, err := NewCipher(CipherParams{Key: make([]byte, 16), KeyBase64: "aa"}); err == nil {
		t.Error("both key forms: want error")
	}
	// The key length decides the real strength, so a contradicting label must fail
	// loudly: a caller asking for AES-256 with a 16-byte key must not silently get
	// AES-128.
	if _, err := NewCipher(CipherParams{Key: make([]byte, 16), Algorithm: AES256GCM}); err == nil {
		t.Error("contradicting algorithm label: want error")
	}
	if _, err := NewCipher(CipherParams{Key: make([]byte, 16), Algorithm: AES128GCM}); err != nil {
		t.Errorf("matching label: %v", err)
	}
}

// TestGenerateRandomKey checks size and base64 validity.
func TestGenerateRandomKey(t *testing.T) {
	for _, bits := range []int{128, 256} {
		key, err := GenerateRandomKey(bits)
		if err != nil {
			t.Fatalf("GenerateRandomKey(%d): %v", bits, err)
		}
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil || len(decoded) != bits/8 {
			t.Errorf("key %q decodes to %d bytes (err %v)", key, len(decoded), err)
		}
		if _, err := NewCipher(CipherParams{KeyBase64: key}); err != nil {
			t.Errorf("generated key rejected: %v", err)
		}
	}
	if _, err := GenerateRandomKey(192); err == nil {
		t.Error("192 bits: want error")
	}
}

// TestIsCipherEncoding checks the encoding-token detection.
func TestIsCipherEncoding(t *testing.T) {
	positives := []string{"cipher+aes-256-gcm/base64", "cipher+aes-128-gcm/base64", "cipher+aes-256-gcm"}
	for _, encoding := range positives {
		if !IsCipherEncoding(encoding) {
			t.Errorf("IsCipherEncoding(%q) = false", encoding)
		}
	}
	negatives := []string{"", "base64", "json", "cipher+chacha20/base64"}
	for _, encoding := range negatives {
		if IsCipherEncoding(encoding) {
			t.Errorf("IsCipherEncoding(%q) = true", encoding)
		}
	}
}
