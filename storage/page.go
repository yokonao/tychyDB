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

type PageOffsets struct {
	//　後ろから何バイトに先頭があるか
	ptrs []uint32
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
	pg := Page{}
	pg.header = newPageHeaderFromBytes(bytes[0:5])
	pg.bb = bytes[5:]
	return pg
}

func newNonLeafPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: false, numOfPtr: 0}
	pg.bb = make([]byte, 0, PageSize)
	return pg
}

func (pg *Page) headerSize() int {
	return 5
}

func (pg *Page) addRecord(bytes []byte) bool {
	if !pg.header.isLeaf {
		return false
	}
	if (pg.headerSize() + len(pg.bb) + len(bytes)) > PageSize {
		return false
	}
	pg.header.numOfPtr += 1
	pg.bb = append(pg.bb, bytes...)
	return true
}
