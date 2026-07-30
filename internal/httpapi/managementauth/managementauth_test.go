package managementauth

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatches(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		provided string
		matches  bool
	}{
		{name: "equal", expected: "management-secret", provided: "management-secret", matches: true},
		{name: "same length mismatch", expected: "management-secret", provided: "management-secrex", matches: false},
		{name: "different length mismatch", expected: "management-secret", provided: "management-secret-extra", matches: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.matches, Matches(test.expected, test.provided))
		})
	}
}
