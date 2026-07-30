package managementauth

import (
	"crypto/sha256"
	"crypto/subtle"
)

// Matches compares management API keys without exposing their length or
// matching prefix through the comparison step.
func Matches(expected string, provided string) bool {
	expectedDigest := sha256.Sum256([]byte(expected))
	providedDigest := sha256.Sum256([]byte(provided))
	return subtle.ConstantTimeCompare(expectedDigest[:], providedDigest[:]) == 1
}
