package storage

import "encoding/binary"

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
	return 4 + uint32(len(rec.data))
}

func (rec Record) toBytes() []byte {
	buf := make([]byte, rec.getSize())
	binary.BigEndian.PutUint32(buf[:IntSize], rec.size)
	copy(buf[4:], rec.data)
	return buf
}

func (rec Record) fromBytes(bytes []byte) Cell {
	rec.size = binary.BigEndian.Uint32(bytes[:IntSize])
	rec.data = bytes[IntSize : IntSize+rec.size]
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
	buf := make([]byte, cell.getSize())
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(cell.key))
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], cell.pageIndex)
	return buf
}

func (cell KeyCell) fromBytes(bytes []byte) Cell {
	cell.key = int32(binary.BigEndian.Uint32(bytes[:IntSize]))
	cell.pageIndex = binary.BigEndian.Uint32(bytes[IntSize : 2*IntSize])
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
	buf := make([]byte, cell.getSize())
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(cell.key))
	copy(buf[IntSize:], cell.rec.toBytes())
	return buf
}

func (cell KeyValueCell) fromBytes(bytes []byte) Cell {
	cell.key = int32(binary.BigEndian.Uint32(bytes[:IntSize]))
	rec := Record{}
	cell.rec = rec.fromBytes(bytes[IntSize:]).(Record)
	return cell
}
