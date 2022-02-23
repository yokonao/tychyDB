package assert

import (
	"testing"
)

func EqualInt32(t *testing.T, x, y int32) {
	if x != y {
		t.Errorf("expected %d, but got %d", x, y)
	}
}

func EqualUInt32(t *testing.T, x, y uint32) {
	if x != y {
		t.Errorf("expected %d, but got %d", x, y)
	}
}

func Equal(t *testing.T, x, y interface{}) {
	if x == nil || y == nil {
		t.Errorf("expected %v, but got %v", x, y)
	}

	switch xval := x.(type) {
	case bool:
		yval, ok := y.(bool)
		if !ok {
			t.Errorf("type missmatch x: %d, y: %v", x, y)
		}
		if xval != yval {
			t.Errorf("expected %v, but got %v", x, y)

		}
	case string:
		yval, ok := y.(string)
		if !ok {
			t.Errorf("type missmatch x: %d, y: %v", x, y)
		}
		if xval != yval {
			t.Errorf("expected %s, but got %s", x, y)
		}
	default:
		t.Errorf("type not supported")
	}

}
