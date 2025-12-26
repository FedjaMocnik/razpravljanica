package subtoken

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type Claims struct {
	UserID    int64   `json:"user_id"`
	TopicIDs  []int64 `json:"topic_ids"`
	NodeID    string  `json:"node_id"`
	IssuedAt  int64   `json:"iat"`
	ExpiresAt int64   `json:"exp"`
}

func Make(secret []byte, claims Claims) (string, error) {
	if claims.IssuedAt == 0 {
		claims.IssuedAt = time.Now().Unix()
	}
	if claims.ExpiresAt == 0 {
		claims.ExpiresAt = claims.IssuedAt + int64((15 * time.Minute).Seconds())
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	sig := mac.Sum(nil)
	p := base64.RawURLEncoding.EncodeToString(payload)
	s := base64.RawURLEncoding.EncodeToString(sig)
	return p + "." + s, nil
}

func ParseAndVerify(secret []byte, token string) (Claims, error) {
	var out Claims
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return out, fmt.Errorf("neveljaven token format")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return out, fmt.Errorf("neveljaven token payload")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return out, fmt.Errorf("neveljaven token podpis")
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(payload)
	expected := mac.Sum(nil)
	if !hmac.Equal(sig, expected) {
		return out, fmt.Errorf("neveljaven token podpis")
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("neveljaven token payload")
	}
	now := time.Now().Unix()
	if out.ExpiresAt > 0 && now > out.ExpiresAt {
		return out, fmt.Errorf("token je potekel")
	}
	return out, nil
}
