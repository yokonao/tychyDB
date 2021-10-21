package storage

import (
	"fmt"
	"strings"

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
	gen := util.NewGenStruct(0, 3*IntSize+c.ty.size)
	gen.PutUInt32(uint32(c.ty.id))
	gen.PutUInt32(c.ty.size)
	gen.PutUInt32(c.pos)
	buf := make([]byte, c.ty.size)
	rd := strings.NewReader(c.name)
	rd.Read(buf)
	gen.PutBytes(c.ty.size, buf)
	return gen.DumpBytes()
}

func newColumnfromBytes(bytes []byte) Column {
	c := Column{}
	iter := util.NewIterStruct(0, bytes)
	c.ty.id = TypeId(iter.NextUInt32())
	c.ty.size = iter.NextUInt32()
	c.pos = iter.NextUInt32()
	c.name = string(iter.NextBytes(c.ty.size))
	return c
}
