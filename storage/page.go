package storage

import (
	"encoding/binary"
	"errors"
	"sort"
)

const PageSize = 4096
const PageHeaderSize = 9

type PageHeader struct {
	isLeaf       bool
	numOfPtr     uint32
	rightmostPtr uint32
}

func (header PageHeader) toBytes() []byte {
	buf := make([]byte, PageHeaderSize)
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
	header PageHeader
	ptrs   []uint32 // cellsのindexを保持する
	cells  []Cell   // [0]が最初に挿入されたセル
}

func newPage() *Page {
	pg := &Page{}
	pg.header = PageHeader{isLeaf: true, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Cell, 0)
	return pg
}

type ptrCellPair struct {
	ptrIndex int
	cellTop  uint32
}

func newPageFromBytes(bytes []byte) *Page {
	if len(bytes) != PageSize {
		panic(errors.New("bytes length must be PageSize"))
	}
	pg := &Page{}
	pg.header = newPageHeaderFromBytes(bytes[:PageHeaderSize])
	ptrCellPairs := make([]ptrCellPair, pg.header.numOfPtr)
	for i := 0; i < int(pg.header.numOfPtr); i++ {
		value := binary.BigEndian.Uint32(bytes[PageHeaderSize+i*4 : PageHeaderSize+(i+1)*4])
		ptrCellPairs[i] = ptrCellPair{ptrIndex: i, cellTop: value}
	}
	sort.Slice(ptrCellPairs, func(i, j int) bool { return ptrCellPairs[i].cellTop > ptrCellPairs[j].cellTop })
	pg.ptrs = make([]uint32, pg.header.numOfPtr)
	for i, item := range ptrCellPairs {
		var cell Cell
		if pg.header.isLeaf {
			cell = Record{}.fromBytes(bytes[item.cellTop:])
		} else {
			cell = KeyCell{}.fromBytes(bytes[item.cellTop:])
		}
		pg.cells = append(pg.cells, cell)
		pg.ptrs[item.ptrIndex] = uint32(i)
	}
	return pg
}

func newNonLeafPage() *Page {
	pg := &Page{}
	pg.header = PageHeader{isLeaf: false, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Cell, 0)
	return pg
}

func (pg *Page) getContentSize() (size uint32) {
	size = 0
	for _, c := range pg.cells {
		size += c.getSize()
	}
	return
}

func (pg *Page) getPageSize() uint32 {
	return PageHeaderSize + 4*pg.header.numOfPtr + pg.getContentSize()
}

func (pg *Page) locateLocally(key int32) uint32 {
	for i, ptr := range pg.ptrs {
		compared := pg.cells[ptr].(KeyCell).key
		if key < compared {
			return uint32(i)
		}
	}
	return pg.header.numOfPtr
}

func (pg *Page) addRecord(rec Record) bool {
	if !pg.header.isLeaf {
		return false
	}
	if (PageSize - pg.getPageSize()) < (4 + rec.getSize()) {
		return false
	}

	pg.ptrs = append(pg.ptrs, pg.header.numOfPtr)
	pg.cells = append(pg.cells, rec)
	pg.header.numOfPtr++
	return true
}

func (pg *Page) addKeyCell(cell KeyCell) {
	if pg.header.isLeaf {
		panic(errors.New("cannot add KeyCell to Leaf Page"))
	}

	if (PageSize - pg.getPageSize()) < (4 + cell.getSize()) {
		panic(errors.New("full root page"))
	}
	idx := pg.locateLocally(cell.key)
	pg.ptrs = append(pg.ptrs, 0)
	copy(pg.ptrs[idx+1:], pg.ptrs[idx:])
	pg.ptrs[idx] = pg.header.numOfPtr
	pg.cells = append(pg.cells, cell)
	pg.header.numOfPtr++
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	copy(buf[:PageHeaderSize], pg.header.toBytes())
	ptrRawValues := make([]uint32, pg.header.numOfPtr)
	cur := PageSize
	for i, cell := range pg.cells {
		copy(buf[cur-int(cell.getSize()):cur], cell.toBytes())
		cur = cur - int(cell.getSize())
		ptrRawValues[i] = uint32(cur)
	}
	for i, ptr := range pg.ptrs {
		rawValue := ptrRawValues[int(ptr)]
		binary.BigEndian.PutUint32(buf[PageHeaderSize+i*4:PageHeaderSize+(i+1)*4], rawValue)
	}
	return buf
}
