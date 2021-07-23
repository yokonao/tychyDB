package storage

import (
	"encoding/binary"
	"errors"
)

const PageSize = 100
const PageHeaderSize = 5

type PageHeader struct {
	// total 5 byte
	isLeaf   bool
	numOfPtr uint32
}

func (header PageHeader) toBytes() []byte {
	buf := make([]byte, 5, 5)
	if header.isLeaf {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.BigEndian.PutUint32(buf[1:5], header.numOfPtr)
	return buf
}

func newPageHeaderFromBytes(bytes []byte) PageHeader {
	if len(bytes) != PageHeaderSize {
		panic(errors.New("bytes length must be PageHeaderSize"))
	}
	isLeaf := bytes[0] == 1
	numOfPtr := binary.BigEndian.Uint32(bytes[1:5])
	return PageHeader{isLeaf: isLeaf, numOfPtr: numOfPtr}
}

type Record struct {
	size uint32
	data []byte
}

func (rec Record) getSize() int {
	return 4 + len(rec.data)
}

func (rec Record) toBytes() []byte {
	buf := make([]byte, rec.getSize())
	binary.BigEndian.PutUint32(buf[:4], rec.size)
	copy(buf[4:], rec.data)
	return buf
}

type Page struct {
	// byte buffer
	header PageHeader
	ptrs   []uint32
	cells  []Record
}

func newPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: true, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Record, 0)
	return pg
}

func newPageFromBytes(bytes []byte) Page {
	if len(bytes) != PageSize {
		panic(errors.New("bytes length must be PageSize"))
	}
	pg := Page{}
	pg.header = newPageHeaderFromBytes(bytes[0:5])
	pg.setPtrsFromBytes(pg.header.numOfPtr, bytes[5:5+4*pg.header.numOfPtr])
	for _, ptr := range pg.ptrs {
		rec := Record{}
		rec.size = binary.BigEndian.Uint32(bytes[ptr : ptr+4])
		rec.data = bytes[ptr+4 : ptr+4+rec.size]
		pg.cells = append(pg.cells, rec)
	}
	return pg
}

func newNonLeafPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: false, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Record, 0)
	return pg
}

func (pg *Page) setPtrsFromBytes(numOfPtr uint32, bytes []byte) {
	if int(numOfPtr*4) != len(bytes) {
		panic(errors.New("bytes length must be 4 * numOfPtr"))
	}
	pg.ptrs = make([]uint32, numOfPtr)
	for i := 0; i < int(numOfPtr); i++ {
		pg.ptrs[i] = binary.BigEndian.Uint32(bytes[i*4 : (i+1)*4])
	}
}

func (pg *Page) headerSize() int {
	return 5
}

func (pg *Page) getContentSize() int {
	size := 0
	for _, c := range pg.cells {
		size += c.getSize()
	}
	return size
}

func (pg *Page) addRecord(rec Record) bool {
	if !pg.header.isLeaf {
		return false
	}
	if pg.headerSize()+4*(int(pg.header.numOfPtr)+1) > PageSize-pg.getContentSize()-rec.getSize() {
		return false
	}
	pg.header.numOfPtr++

	pg.ptrs = append(pg.ptrs, uint32(PageSize-pg.getContentSize()-rec.getSize()))
	pg.cells = append(pg.cells, rec)

	return true
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	copy(buf[:5], pg.header.toBytes())
	for i, ptr := range pg.ptrs {
		binary.BigEndian.PutUint32(buf[5+i*4:5+(i+1)*4], ptr)
		copy(buf[ptr:ptr+uint32(pg.cells[i].getSize())], pg.cells[i].toBytes())
	}
	return buf
}
