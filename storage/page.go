package storage

import (
	"encoding/binary"
	"errors"
)

const PageSize = 100

type PageHeader struct {
	// total 5 byte
	isLeaf   bool
	numOfPtr uint32
}
type Page struct {
	// byte buffer
	header PageHeader
	bb     []byte
}

func newPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: true, numOfPtr: 0}
	pg.bb = make([]byte, 0, PageSize)
	return pg
}

func newPageFromBytes(bytes []byte) Page {
	if len(bytes) != PageSize {
		panic(errors.New("bytes length must be PageSize"))
	}
	isLeaf := bytes[0] == 0
	numOfPtr := binary.BigEndian.Uint32(bytes[1:5])
	pg := Page{}
	pg.header = PageHeader{isLeaf: isLeaf, numOfPtr: numOfPtr}
	pg.bb = bytes[5:]
	return pg
}

func (pg *Page) headerSize() int {
	return 5
}

func (pg *Page) getPageLength() int {
	return pg.headerSize() + len(pg.bb)
}

func (pg *Page) setBytes(bytes []byte) bool {
	if (pg.getPageLength() + len(bytes)) > PageSize {
		return false
	}
	pg.header.numOfPtr += 1
	pg.bb = append(pg.bb, bytes...)
	return true
}

func (pg *Page) getInt(offset int) uint32 {
	return binary.BigEndian.Uint32(pg.bb[offset : offset+32]) // 32byte integer
}

func (pg *Page) setInt(offset int, n uint32) {
	binary.BigEndian.PutUint32(pg.bb[offset:], n)

}
