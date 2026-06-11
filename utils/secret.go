package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"strings"

	"github.com/wfu-work/nav-common-go-lib/global"
)

const encryptedPrefix = "enc:v1:"

func EncryptSecret(plain string) (string, error) {
	return EncryptSecretWithKey(plain, runtimeSecretKey())
}

func DecryptSecret(value string) (string, error) {
	return DecryptSecretWithKey(value, runtimeSecretKey())
}

func MaskSecret(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "******"
}

func IsEncryptedSecret(value string) bool {
	return strings.HasPrefix(strings.TrimSpace(value), encryptedPrefix)
}

func EncryptSecretWithKey(plain string, key string) (string, error) {
	if plain == "" {
		return "", nil
	}
	if IsEncryptedSecret(plain) {
		return plain, nil
	}
	aead, err := secretAEAD(key)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	cipherText := aead.Seal(nil, nonce, []byte(plain), nil)
	payload := append(nonce, cipherText...)
	return encryptedPrefix + base64.RawURLEncoding.EncodeToString(payload), nil
}

func DecryptSecretWithKey(value string, key string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	if !IsEncryptedSecret(value) {
		return value, nil
	}
	aead, err := secretAEAD(key)
	if err != nil {
		return "", err
	}
	raw := strings.TrimPrefix(value, encryptedPrefix)
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return "", err
	}
	if len(payload) <= aead.NonceSize() {
		return "", errors.New("encrypted secret payload invalid")
	}
	nonce := payload[:aead.NonceSize()]
	cipherText := payload[aead.NonceSize():]
	plain, err := aead.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func secretAEAD(key string) (cipher.AEAD, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, errors.New("secret key required")
	}
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func runtimeSecretKey() string {
	if global.NAV_VIPER != nil {
		if value := strings.TrimSpace(global.NAV_VIPER.GetString("security.secret-key")); value != "" {
			return value
		}
		if value := strings.TrimSpace(global.NAV_VIPER.GetString("jwt.signing-key")); value != "" {
			return value
		}
	}
	return "datasync"
}
