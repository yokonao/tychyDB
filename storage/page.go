package storage

import (
	"encoding/binary"
)

const PageSize = 100

type PageHeader struct {
	// total 5 byte
	isLeaf bool
	numPtr int32
}
type Page struct {
	// byte buffer
	header PageHeader
	bb     []byte
}

func newPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: true, numPtr: 0}
	pg.bb = make([]byte, 0, PageSize)
	return pg
}

func (pg *Page) setBytes(bytes []byte) bool {
	if len(pg.bb) == PageSize {
		return false
	}
	pg.bb = append(pg.bb, bytes...)
	return true
}

func (pg *Page) getInt(offset int) uint32 {
	return binary.BigEndian.Uint32(pg.bb[offset : offset+32]) // 32byte integer
}

func (pg *Page) setInt(offset int, n uint32) {
	binary.BigEndian.PutUint32(pg.bb[offset:], n)

}
