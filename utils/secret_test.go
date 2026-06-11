package utils

import "testing"

func TestEncryptDecryptSecretWithKey(t *testing.T) {
	encrypted, err := EncryptSecretWithKey("secret-value", "test-key")
	if err != nil {
		t.Fatalf("encrypt secret failed: %v", err)
	}
	if encrypted == "secret-value" {
		t.Fatal("encrypted secret should not equal plain value")
	}
	if !IsEncryptedSecret(encrypted) {
		t.Fatalf("encrypted secret should use encrypted prefix: %s", encrypted)
	}
	plain, err := DecryptSecretWithKey(encrypted, "test-key")
	if err != nil {
		t.Fatalf("decrypt secret failed: %v", err)
	}
	if plain != "secret-value" {
		t.Fatalf("unexpected decrypted secret: %s", plain)
	}
}

func TestDecryptSecretWithKeyAllowsPlaintext(t *testing.T) {
	plain, err := DecryptSecretWithKey("legacy-secret", "test-key")
	if err != nil {
		t.Fatalf("decrypt plaintext failed: %v", err)
	}
	if plain != "legacy-secret" {
		t.Fatalf("unexpected plaintext value: %s", plain)
	}
}
