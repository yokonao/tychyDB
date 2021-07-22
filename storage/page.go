package storage

import (
	"encoding/binary"
)

const PageSize = 100

type Page struct {
	// byte buffer
	bb []byte
}

func newPage() Page {
	pg := Page{}
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
