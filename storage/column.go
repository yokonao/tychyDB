package storage

import (
	"encoding/binary"
	"errors"
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
	gen := util.NewGenStruct(0, 4*IntSize+c.ty.size)
	gen.PutUInt32(uint32(c.ty.id))
	gen.PutUInt32(c.ty.size)
	gen.PutUInt32(c.pos)
	gen.PutStringWithSize(c.name, c.ty.size)
	return gen.DumpBytes()
}

func newColumnfromBytes(bytes []byte) Column {
	c := Column{}
	iter := util.NewIterStruct(0, bytes)
	c.ty.id = TypeId(iter.NextUInt32())
	c.ty.size = iter.NextUInt32()
	c.pos = iter.NextUInt32()
	c.name = iter.NextStringWithSize(c.ty.size)
	return c
}

func encode(cols []Column, args ...interface{}) (bytes []byte, err error) {
	if len(args) != len(cols) {
		err = errors.New("the count of arguments must be same column's")
		return
	}
	bytes = []byte{}
	for i, col := range cols {
		if col.ty.id == integerId {
			val := uint32(args[i].(int))
			buf := make([]byte, col.ty.size)
			binary.BigEndian.PutUint32(buf, val)
			bytes = append(bytes, buf...)
		} else if col.ty.id == charId {
			rd := strings.NewReader(args[i].(string))
			buf := make([]byte, col.ty.size)
			rd.Read(buf)
			bytes = append(bytes, buf...)
		} else {
			bytes = nil
			err = errors.New("the type of a column is not implemented")
			return
		}
	}
	return
}
