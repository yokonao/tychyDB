package storage

import "encoding/binary"

type Cell interface {
	getSize() uint32
	toBytes() []byte
	fromBytes([]byte) Cell
}

type Record struct {
	size uint32
	data []byte
}

func (rec Record) getSize() uint32 {
	return 4 + uint32(len(rec.data))
}

func (rec Record) toBytes() []byte {
	buf := make([]byte, rec.getSize())
	binary.BigEndian.PutUint32(buf[:4], rec.size)
	copy(buf[4:], rec.data)
	return buf
}

func (rec Record) fromBytes(bytes []byte) Cell {
	rec.size = binary.BigEndian.Uint32(bytes[:4])
	rec.data = bytes[4 : 4+rec.size]
	return rec
}

func (rec Record) getKey() int32 {
	// 暫定的に第1カラムの値をキーとして扱う
	return int32(binary.BigEndian.Uint32(rec.data[:4]))
}

const KeyCellSize = 12

type KeyCell struct {
	key       int32
	pageIndex uint32
}

func (cell KeyCell) getSize() uint32 {
	return KeyCellSize
}

func (cell KeyCell) toBytes() []byte {
	buf := make([]byte, cell.getSize())
	binary.BigEndian.PutUint32(buf[:4], uint32(cell.key))
	binary.BigEndian.PutUint32(buf[4:8], cell.pageIndex)
	return buf
}

func (cell KeyCell) fromBytes(bytes []byte) Cell {
	cell.key = int32(binary.BigEndian.Uint32(bytes[:4]))
	cell.pageIndex = binary.BigEndian.Uint32(bytes[4:8])
	return cell
}

type KeyValueCell struct {
	key int32
	rec Record
}

func (cell KeyValueCell) getSize() uint32 {
	return cell.rec.getSize() + 4
}

func (cell KeyValueCell) toBytes() []byte {
	buf := make([]byte, cell.getSize())
	binary.BigEndian.PutUint32(buf[:4], uint32(cell.key))
	copy(buf[4:], cell.rec.toBytes())
	return buf
}

func (cell KeyValueCell) fromBytes(bytes []byte) Cell {
	cell.key = int32(binary.BigEndian.Uint32(bytes[:4]))
	rec := Record{}
	cell.rec = rec.fromBytes(bytes[4:]).(Record)
	return cell
}
