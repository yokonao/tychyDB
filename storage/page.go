package storage

import (
	"encoding/binary"
	"errors"
)

const PageSize = 4096
const PageHeaderSize = 5

type PageHeader struct {
	// total 5 byte
	isLeaf   bool
	numOfPtr uint32
}

func (header PageHeader) toBytes() []byte {
	buf := make([]byte, 5)
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

type Page struct {
	// byte buffer
	header PageHeader
	ptrs   []uint32
	cells  []Cell
}

func newPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: true, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Cell, 0)
	return pg
}

func newPageFromBytes(bytes []byte) Page {
	if len(bytes) != PageSize {
		panic(errors.New("bytes length must be PageSize"))
	}
	pg := Page{}
	pg.header = newPageHeaderFromBytes(bytes[:5])
	pg.setPtrsFromBytes(pg.header.numOfPtr, bytes[5:5+4*pg.header.numOfPtr])
	min := uint32(PageSize)
	for _, ptr := range pg.ptrs {
		if ptr < uint32(min) {
			min = ptr
		}
		// var cell Cell
		// if pg.header.isLeaf {
		// 	cell = Record{}.fromBytes(bytes[ptr:])
		// } else {
		// 	cell = KeyCell{}.fromBytes(bytes[ptr:])
		// }
		// pg.cells = append(pg.cells, cell)
	}
	cur := min
	for i := 0; i < int(pg.header.numOfPtr); i++ {
		var cell Cell
		if pg.header.isLeaf {
			cell = Record{}.fromBytes(bytes[cur:])
		} else {
			cell = KeyCell{}.fromBytes(bytes[cur:])
		}
		pg.cells = append(pg.cells, cell)
		cur += cell.getSize()
	}
	return pg
}

func newNonLeafPage() Page {
	pg := Page{}
	pg.header = PageHeader{isLeaf: false, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Cell, 0)
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

func (pg *Page) headerSize() uint32 {
	return 5
}

func (pg *Page) getContentSize() (size uint32) {
	size = 0
	for _, c := range pg.cells {
		size += c.getSize()
	}
	return
}

func (pg *Page) addRecord(rec Record) (bool, uint32) {
	if !pg.header.isLeaf {
		return false, 0
	}
	if pg.headerSize()+4*(pg.header.numOfPtr+1) > PageSize-pg.getContentSize()-rec.getSize() {
		return false, 0
	}
	pg.header.numOfPtr++

	pg.ptrs = append(pg.ptrs, uint32(PageSize-pg.getContentSize()-rec.getSize()))
	pg.cells = append(pg.cells, rec)

	return true, pg.header.numOfPtr - 1
}

func (pg *Page) locateLocally(key int32) (res uint32) {
	res = pg.header.numOfPtr
	for i, ptr := range pg.ptrs {
		idx := ((PageSize - ptr) / KeyCellSize) - 1
		compared := pg.cells[idx].(KeyCell).key
		if key < compared {
			return uint32(i)
		}
	}
	return
}

func (pg *Page) addKeyCell(cell KeyCell) {
	if pg.header.isLeaf {
		panic(errors.New("cannot add KeyCell to Leaf Page"))
	}
	if pg.headerSize()+4*(pg.header.numOfPtr+1) > PageSize-pg.getContentSize()-cell.getSize() {
		panic(errors.New("full root page"))
	}
	idx := pg.locateLocally(cell.key)
	pg.ptrs = append(pg.ptrs, 0)
	copy(pg.ptrs[idx+1:], pg.ptrs[idx:])
	pg.ptrs[idx] = uint32(PageSize - pg.getContentSize() - cell.getSize())
	pg.cells = append(pg.cells, cell)
	pg.header.numOfPtr++
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	copy(buf[:5], pg.header.toBytes())
	for i, ptr := range pg.ptrs {
		binary.BigEndian.PutUint32(buf[5+i*4:5+(i+1)*4], ptr)
		// copy(buf[ptr:ptr+uint32(pg.cells[i].getSize())], pg.cells[i].toBytes())
	}
	cur := PageSize
	for _, cell := range pg.cells {
		copy(buf[cur-int(cell.getSize()):cur], cell.toBytes())
		cur = cur - int(cell.getSize())
	}
	return buf
}
