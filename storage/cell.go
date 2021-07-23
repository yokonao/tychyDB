package storage

import "encoding/binary"

type Cell interface {
	getSize() uint32
	toBytes() []byte
	fromBytes([]byte)
}

type Record struct {
	size uint32
	data []byte
}

func (rec *Record) getSize() uint32 {
	return 4 + uint32(len(rec.data))
}

func (rec *Record) toBytes() []byte {
	buf := make([]byte, rec.getSize())
	binary.BigEndian.PutUint32(buf[:4], rec.size)
	copy(buf[4:], rec.data)
	return buf
}

func (rec *Record) fromBytes(bytes []byte) {
	rec.size = binary.BigEndian.Uint32(bytes[:4])
	rec.data = bytes[4 : 4+rec.size]
}

type KeyCell struct {
	key       int32
	pageIndex uint32
	ptrIndex  uint32
}
