package storage

import (
	"encoding/binary"

	"github.com/tychyDB/util"
)

type Cell interface {
	getSize() uint32
	getKey() int32
	toBytes() []byte
	fromBytes([]byte) Cell
}

type Record struct {
	size uint32
	data []byte
}

func (rec Record) getSize() uint32 {
	return IntSize + uint32(len(rec.data))
}

func (rec Record) toBytes() []byte {
	gen := util.NewGenStruct(0, rec.getSize())
	gen.PutUInt32(rec.size)
	gen.PutBytes(rec.size, rec.data)
	return gen.DumpBytes()
}

func (rec Record) fromBytes(bytes []byte) Cell {
	iter := util.NewIterStruct(0, bytes)
	rec.size = iter.NextUInt32()
	rec.data = iter.NextBytes(rec.size)
	return rec
}

func (rec Record) getKey() int32 {
	// 暫定的に第1カラムの値をキーとして扱う
	return int32(binary.BigEndian.Uint32(rec.data[:IntSize]))
}

const KeyCellSize = 12

type KeyCell struct {
	key       int32
	pageIndex uint32
}

func (cell KeyCell) getSize() uint32 {
	return KeyCellSize
}

func (cell KeyCell) getKey() int32 {
	return cell.key
}

func (cell KeyCell) toBytes() []byte {
	gen := util.NewGenStruct(0, cell.getSize())
	gen.PutUInt32(uint32(cell.key))
	gen.PutUInt32(cell.pageIndex)
	return gen.DumpBytes()
}

func (cell KeyCell) fromBytes(bytes []byte) Cell {
	iter := util.NewIterStruct(0, bytes)
	cell.key = int32(iter.NextUInt32())
	cell.pageIndex = iter.NextUInt32()
	return cell
}

type KeyValueCell struct {
	key int32
	rec Record
}

func (cell KeyValueCell) getSize() uint32 {
	return cell.rec.getSize() + IntSize
}

func (cell KeyValueCell) getKey() int32 {
	return cell.key
}

func (cell KeyValueCell) toBytes() []byte {
	gen := util.NewGenStruct(0, cell.getSize())
	gen.PutUInt32(uint32(cell.key))
	bytes := cell.rec.toBytes()
	gen.PutBytes(uint32(len(bytes)), bytes)
	return gen.DumpBytes()
}

func (cell KeyValueCell) fromBytes(bytes []byte) Cell {
	cell.key = int32(binary.BigEndian.Uint32(bytes[:IntSize]))
	rec := Record{}
	cell.rec = rec.fromBytes(bytes[IntSize:]).(Record)
	return cell
}
