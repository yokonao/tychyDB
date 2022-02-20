package assert

import (
	"testing"
)

func EqualUint32(t *testing.T, x, y uint32) {
	if x != y {
		t.Errorf("expected %d, but got %d", x, y)
	}
}

func EqualString(t *testing.T, x, y string) {
	if x != y {
		t.Errorf("expected %s, but got %s", x, y)
	}
}

func EqualBool(t *testing.T, x, y bool) {
	if x != y {
		t.Errorf("expected %v, but got %v", x, y)
	}
}
