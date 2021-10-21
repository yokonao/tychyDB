package util_test

import (
	"encoding/binary"
	"testing"

	"github.com/tychyDB/util"
)

func TestIterUInt32(t *testing.T) {
	bytes := make([]byte, 12)
	binary.BigEndian.PutUint32(bytes[:util.IntSize], 10)
	binary.BigEndian.PutUint32(bytes[util.IntSize:2*util.IntSize], 16)
	binary.BigEndian.PutUint32(bytes[2*util.IntSize:3*util.IntSize], 30)

	iter := util.NewIterStruct(0, bytes)

	if i := iter.NextUInt32(); i != 10 {
		t.Errorf("expected %d, but got %d", 10, i)
	}
	if i := iter.NextUInt32(); i != 16 {
		t.Errorf("expected %d, but got %d", 16, i)
	}

	if i := iter.NextUInt32(); i != 30 {
		t.Errorf("expected %d, but got %d", 30, i)
	}
}
func TestIterBool(t *testing.T) {
	bytes := make([]byte, 12)
	bytes[0] = 1
	binary.BigEndian.PutUint32(bytes[util.BoolSize:util.BoolSize+util.IntSize], 16)
	bytes[5] = 0

	iter := util.NewIterStruct(0, bytes)

	if i := iter.NextBool(); !i {
		t.Errorf("expected %v, but got %v", true, i)
	}

	if i := iter.NextUInt32(); i != 16 {
		t.Errorf("expected %d, but got %d", 16, i)
	}

	if i := iter.NextBool(); i {
		t.Errorf("expected %v, but got %v", false, i)
	}
}
func TestIterBytes(t *testing.T) {
	bytes := make([]byte, 12)
	binary.BigEndian.PutUint32(bytes[:util.IntSize], 10)
	binary.BigEndian.PutUint32(bytes[util.IntSize:2*util.IntSize], 16)
	binary.BigEndian.PutUint32(bytes[2*util.IntSize:3*util.IntSize], 30)
	ans := make([]byte, 5)
	copy(ans, bytes[:5])

	iter := util.NewIterStruct(0, bytes)
	b := iter.NextBytes(5)
	for i := 0; i < 5; i++ {
		if b[i] != ans[i] {
			t.Error("byte mismatch")
		}
	}
}
