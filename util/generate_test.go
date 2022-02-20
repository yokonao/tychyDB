package util_test

import (
	"testing"

	"github.com/tychyDB/assert"
	"github.com/tychyDB/util"
)

func TestGenUInt32(t *testing.T) {
	gs := util.NewGenStruct(0, 100)
	gs.PutUInt32(10)
	gs.PutUInt32(11)
	gs.PutUInt32(111)

	iter := util.NewIterStruct(0, gs.DumpBytes())
	assert.EqualUint32(t, iter.NextUInt32(), 10)
	assert.EqualUint32(t, iter.NextUInt32(), 11)
	assert.EqualUint32(t, iter.NextUInt32(), 111)
}

func TestGenBool(t *testing.T) {
	gs := util.NewGenStruct(0, 100)
	gs.PutUInt32(10)
	gs.PutBool(false)
	gs.PutUInt32(11)
	gs.PutBool(true)
	gs.PutUInt32(111)
	gs.PutBool(true)

	iter := util.NewIterStruct(0, gs.DumpBytes())
	assert.EqualUint32(t, iter.NextUInt32(), 10)
	assert.EqualBool(t, iter.NextBool(), false)
	assert.EqualUint32(t, iter.NextUInt32(), 11)
	assert.EqualBool(t, iter.NextBool(), true)
	assert.EqualUint32(t, iter.NextUInt32(), 111)
	assert.EqualBool(t, iter.NextBool(), true)
}

func TestGenBytes(t *testing.T) {
	t_gs := util.NewGenStruct(0, 12)
	t_gs.PutUInt32(10)
	t_gs.PutUInt32(11)
	t_gs.PutUInt32(111)

	gs := util.NewGenStruct(0, 3232)
	gs.PutUInt32(11)
	gs.PutBytes(12, t_gs.DumpBytes())
	gs.PutUInt32(22)
	gs.PutBytes(12, t_gs.DumpBytes())

	iter := util.NewIterStruct(0, gs.DumpBytes())
	assert.EqualUint32(t, iter.NextUInt32(), 11)
	assert.EqualUint32(t, iter.NextUInt32(), 10)
	assert.EqualUint32(t, iter.NextUInt32(), 11)
	assert.EqualUint32(t, iter.NextUInt32(), 111)
	assert.EqualUint32(t, iter.NextUInt32(), 22)
	assert.EqualUint32(t, iter.NextUInt32(), 10)
	assert.EqualUint32(t, iter.NextUInt32(), 11)
	assert.EqualUint32(t, iter.NextUInt32(), 111)
}

func TestPutUInt32WithSize(t *testing.T) {
	gs := util.NewGenStruct(0, 100)
	gs.PutStringWithSize("hoge", 10)

	iter := util.NewIterStruct(0, gs.DumpBytes())
	assert.EqualString(t, iter.NextStringWithSize(10), "hoge")
}

func TestToByteStringWithSize(t *testing.T) {
	bytes := util.ToByteStringWithSize("hogehoge", 8)

	iter := util.NewIterStruct(0, bytes)
	assert.EqualString(t, iter.NextStringWithSize(8), "hogehoge")
}
