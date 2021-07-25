package storage

import (
	"encoding/binary"
	"errors"
	"sort"
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
	header PageHeader
	ptrs   []uint32 // cellsのindexを保持する
	cells  []Cell   // [0]が最初に挿入されたセル
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
	ptrRawValues := make(map[uint32]int, pg.header.numOfPtr)
	ptrRawValuesSorted := make([]uint32, pg.header.numOfPtr)
	for i := 0; i < int(pg.header.numOfPtr); i++ {
		value := binary.BigEndian.Uint32(bytes[int(pg.headerSize())+i*4 : int(pg.headerSize())+(i+1)*4])
		ptrRawValues[value] = i
		ptrRawValuesSorted[i] = value
	}
	sort.Slice(ptrRawValuesSorted, func(i, j int) bool { return ptrRawValuesSorted[i] > ptrRawValuesSorted[j] })
	pg.ptrs = make([]uint32, pg.header.numOfPtr)
	for i, ptr := range ptrRawValuesSorted {
		var cell Cell
		if pg.header.isLeaf {
			cell = Record{}.fromBytes(bytes[ptr:])
		} else {
			cell = KeyCell{}.fromBytes(bytes[ptr:])
		}
		pg.cells = append(pg.cells, cell)
		pg.ptrs[ptrRawValues[ptr]] = uint32(i)
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

	pg.ptrs = append(pg.ptrs, pg.header.numOfPtr)
	pg.cells = append(pg.cells, rec)
	pg.header.numOfPtr++
	return true, pg.header.numOfPtr - 1
}

func (pg *Page) locateLocally(key int32) (res uint32) {
	res = pg.header.numOfPtr
	for i, ptr := range pg.ptrs {
		compared := pg.cells[ptr].(KeyCell).key
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
	pg.ptrs[idx] = pg.header.numOfPtr
	pg.cells = append(pg.cells, cell)
	pg.header.numOfPtr++
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	copy(buf[:5], pg.header.toBytes())
	ptrRawValues := make([]uint32, pg.header.numOfPtr)
	cur := PageSize
	for i, cell := range pg.cells {
		copy(buf[cur-int(cell.getSize()):cur], cell.toBytes())
		cur = cur - int(cell.getSize())
		ptrRawValues[i] = uint32(cur)
	}
	for i, ptr := range pg.ptrs {
		rawValue := ptrRawValues[int(ptr)]
		binary.BigEndian.PutUint32(buf[5+i*4:5+(i+1)*4], rawValue)
	}
	return buf
}
