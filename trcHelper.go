package rmqevnter

import (
	"crypto/rand"
	"encoding/hex"
)

func GenerateRadomHexString(n int) (string, error) {
	buff := make([]byte, n)
	if _, err := rand.Read(buff); err != nil {
		return "", err
	} else {
		return hex.EncodeToString(buff), nil
	}
}
