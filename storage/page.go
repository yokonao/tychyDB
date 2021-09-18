package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

const PageSize = 4096
const PageHeaderSize = 9
const MaxDegree = 3

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
		var compared int32
		if pg.header.isLeaf {
			compared = pg.cells[ptr].(KeyValueCell).key
		} else {
			compared = pg.cells[ptr].(KeyCell).key
		}
		if key < compared {
			return uint32(i)
		}
	}
	return pg.header.numOfPtr
}
func insertInt(index int, item uint32, arr []uint32) []uint32 {
	arr = append(arr, 0)
	copy(arr[index+1:], arr[index:])
	arr[index] = item
	return arr
}

func (pg *Page) addRecordRec(rec Record) (splitted bool, splitKey int32, leftPageIndex uint32) {
	key := rec.getKey()
	insert_idx := pg.locateLocally(key)
	if pg.header.isLeaf {
		if len(pg.ptrs) == 0 {
			pg.ptrs = append(pg.ptrs, 0)
			pg.cells = append(pg.cells, KeyValueCell{key: key, rec: rec})
			pg.header.numOfPtr++
		} else {
			pg.ptrs = insertInt(int(insert_idx), pg.header.numOfPtr, pg.ptrs)
			pg.cells = append(pg.cells, KeyValueCell{key: key, rec: rec})
			pg.header.numOfPtr++
		}
	} else {
		var pageIndex uint32
		if insert_idx == pg.header.numOfPtr {
			pageIndex = pg.cells[pg.header.rightmostPtr].(KeyCell).pageIndex
		} else {
			cellIndex := pg.ptrs[insert_idx]
			pageIndex = pg.cells[cellIndex].(KeyCell).pageIndex
		}
		blk := newBlockId(pageIndex)
		splitted, splitKey, leftPageIndex := bm.pool[ptb.getBuffId(blk)].addRecordRec(rec)
		if splitted {
			if insert_idx == pg.header.numOfPtr {
				insert_idx--
			}
			pg.ptrs = insertInt(int(insert_idx), pg.header.numOfPtr, pg.ptrs)
			pg.cells = append(pg.cells, KeyCell{key: splitKey, pageIndex: leftPageIndex})
			pg.header.numOfPtr++
		}
	}
	if pg.header.isLeaf && pg.header.numOfPtr >= MaxDegree {
		splitted = true
		splitIndex := pg.header.numOfPtr / 2
		splitKey = pg.cells[pg.ptrs[splitIndex]].(KeyValueCell).key
		leftPage := newPage()
		blk := newUniqueBlockId()
		ptb.set(blk, leftPage)
		leftPageIndex = blk.blockNum
		leftPage.ptrs = make([]uint32, splitIndex)
		leftPage.cells = make([]Cell, splitIndex)
		for i := 0; i < int(splitIndex); i++ {
			leftPage.ptrs[i] = uint32(i)
			leftPage.cells[i] = pg.cells[pg.ptrs[i]]
		}
		leftPage.header.numOfPtr = splitIndex
		pg.ptrs = pg.ptrs[splitIndex:]
		pg.header.numOfPtr -= splitIndex
	} else if !pg.header.isLeaf && pg.header.numOfPtr > MaxDegree {
		splitted = true
		splitIndex := (pg.header.numOfPtr - 1) / 2
		splitKey = pg.cells[pg.ptrs[splitIndex]].(KeyCell).key
		leftPage := newNonLeafPage()
		blk := newUniqueBlockId()
		ptb.set(blk, leftPage)
		leftPageIndex = blk.blockNum
		leftPage.ptrs = make([]uint32, splitIndex-1)
		leftPage.cells = make([]Cell, splitIndex)
		for i := 0; i < int(splitIndex-1); i++ {
			leftPage.ptrs[i] = uint32(i)
			leftPage.cells[i] = pg.cells[pg.ptrs[i]]
		}
		leftPage.cells[splitIndex-1] = pg.cells[pg.ptrs[splitIndex-1]]
		leftPage.header.rightmostPtr = splitIndex - 1
		leftPage.header.numOfPtr = splitIndex
		pg.ptrs = pg.ptrs[splitIndex:]
		pg.header.numOfPtr -= splitIndex
	} else {
		splitted = false
	}
	return
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	copy(buf[:PageHeaderSize], pg.header.toBytes())
	ptrRawValues := make([]uint32, len(pg.cells))
	cur := PageSize
	for i, ptr := range pg.ptrs {
		cell := pg.cells[pg.ptrs[i]]

		copy(buf[cur-int(cell.getSize()):cur], cell.toBytes())
		cur = cur - int(cell.getSize())
		ptrRawValues[ptr] = uint32(cur)
	}

	for i := range pg.ptrs {
		rawValue := ptrRawValues[int(i)]
		binary.BigEndian.PutUint32(buf[PageHeaderSize+i*4:PageHeaderSize+(i+1)*4], rawValue)
	}

	return buf
}

func (pg *Page) info() {
	fmt.Printf("Page Info ... \n")
	fmt.Printf("| isLeaf %v\n", pg.header.isLeaf)
	fmt.Printf("| numofptrs ... %d\n", pg.header.numOfPtr)
	fmt.Printf("| len(ptrs) %d, ptrs... %v\n", len(pg.ptrs), pg.ptrs)
	fmt.Printf("| rightmost ptr ... %d\n", pg.header.rightmostPtr)
	fmt.Printf("| len(cells) %d, cells... %v\n", len(pg.cells), pg.cells)
}
