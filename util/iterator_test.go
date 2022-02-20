package util_test

import (
	"encoding/binary"
	"testing"

	"github.com/tychyDB/assert"
	"github.com/tychyDB/util"
)

func TestIterUInt32(t *testing.T) {
	bytes := make([]byte, 12)
	binary.BigEndian.PutUint32(bytes[:util.IntSize], 10)
	binary.BigEndian.PutUint32(bytes[util.IntSize:2*util.IntSize], 16)
	binary.BigEndian.PutUint32(bytes[2*util.IntSize:3*util.IntSize], 30)

	iter := util.NewIterStruct(0, bytes)
	assert.EqualUint32(t, iter.NextUInt32(), 10)
	assert.EqualUint32(t, iter.NextUInt32(), 16)
	assert.EqualUint32(t, iter.NextUInt32(), 30)
}

func TestIterBool(t *testing.T) {
	bytes := make([]byte, 12)
	bytes[0] = 1
	binary.BigEndian.PutUint32(bytes[util.BoolSize:util.BoolSize+util.IntSize], 16)
	bytes[5] = 0

	iter := util.NewIterStruct(0, bytes)
	assert.EqualBool(t, iter.NextBool(), true)
	assert.EqualUint32(t, iter.NextUInt32(), 16)
	assert.EqualBool(t, iter.NextBool(), false)
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
