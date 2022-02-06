package storage

import (
	"fmt"

	"github.com/tychyDB/util"
)

type Column struct {
	ty   Type
	pos  uint32
	name string
}

func (c Column) String() string {
	return fmt.Sprintf("{ type: %s, name: %s }", c.ty, c.name)
}

func (c Column) toBytes() []byte {
	nameSize := len(c.name)
	gen := util.NewGenStruct(0, 4*IntSize+uint32(nameSize))
	gen.PutUInt32(uint32(c.ty.id))
	gen.PutUInt32(c.ty.size)
	gen.PutUInt32(c.pos)
	gen.PutStringWithSize(c.name) // this uses 4 + len(c.name)
	return gen.DumpBytes()
}

func newColumnfromBytes(bytes []byte) Column {
	c := Column{}
	iter := util.NewIterStruct(0, bytes)
	c.ty.id = TypeId(iter.NextUInt32())
	c.ty.size = iter.NextUInt32()
	c.pos = iter.NextUInt32()
	c.name = iter.NextStringWithSize()
	return c
}
