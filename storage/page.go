package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tychyDB/util"
)

const PageSize = 4096
const PageHeaderSize = 17
const MaxDegree = 3
const IntSize = 4

type PageHeader struct {
	isLeaf       bool
	numOfPtr     uint32
	rightmostPtr uint32
	pageLSN      uint32
	recLSN       uint32
}

func (header PageHeader) toBytes() []byte {
	gen := util.NewGenStruct(0, PageHeaderSize)
	gen.PutBool(header.isLeaf)
	gen.PutUInt32(header.numOfPtr)
	gen.PutUInt32(header.rightmostPtr) // dummy value is okay
	gen.PutUInt32(header.pageLSN)
	gen.PutUInt32(header.recLSN)
	return gen.DumpBytes()
}

func (header PageHeader) toBytesNonLeaf(rightmostPtrValue uint32) []byte {
	gen := util.NewGenStruct(0, PageHeaderSize)
	gen.PutBool(header.isLeaf)
	gen.PutUInt32(header.numOfPtr)
	gen.PutUInt32(rightmostPtrValue)
	gen.PutUInt32(header.pageLSN)
	gen.PutUInt32(header.recLSN)
	return gen.DumpBytes()
}

func newPageHeaderFromBytes(bytes []byte) PageHeader {
	if len(bytes) != PageHeaderSize {
		panic(errors.New("bytes length must be PageHeaderSize"))
	}
	iter := util.NewIterStruct(0, bytes)
	isLeaf := iter.NextBool()
	numOfPtr := iter.NextUInt32()
	rightmostPtr := iter.NextUInt32() // ここで読んだときにはまだディスク上の4096byteのどこからcellが始まるかを示している
	pageLSN := iter.NextUInt32()
	recLSN := iter.NextUInt32()
	return PageHeader{isLeaf: isLeaf, numOfPtr: numOfPtr, rightmostPtr: rightmostPtr, pageLSN: pageLSN, recLSN: recLSN}
}

type Page struct {
	header PageHeader
	ptrs   []uint32 // cellsのindexを保持する
	cells  []Cell   // [0]が最初に挿入されたセル
}

func newPage(isLeaf bool) *Page {
	pg := &Page{}
	pg.header = PageHeader{isLeaf: isLeaf, numOfPtr: 0}
	pg.ptrs = make([]uint32, 0)
	pg.cells = make([]Cell, 0)
	return pg
}

func newPageFromBytes(bytes []byte) *Page {
	if len(bytes) != PageSize {
		panic(errors.New("bytes length must be PageSize"))
	}
	pg := &Page{}
	pg.header = newPageHeaderFromBytes(bytes[:PageHeaderSize])

	if pg.header.isLeaf {
		pg.ptrs = make([]uint32, pg.header.numOfPtr)
		for i := 0; i < int(pg.header.numOfPtr); i++ {
			value := binary.BigEndian.Uint32(bytes[PageHeaderSize+i*IntSize : PageHeaderSize+(i+1)*IntSize])
			cell := KeyValueCell{}.fromBytes(bytes[value:])
			pg.ptrs[i] = uint32(i)
			pg.cells = append(pg.cells, cell)
		}

	} else {
		pg.ptrs = make([]uint32, pg.header.numOfPtr-1)
		for i := 0; i < int(pg.header.numOfPtr-1); i++ {
			value := binary.BigEndian.Uint32(bytes[PageHeaderSize+i*IntSize : PageHeaderSize+(i+1)*IntSize])
			cell := KeyCell{}.fromBytes(bytes[value:])
			pg.ptrs[i] = uint32(i)
			pg.cells = append(pg.cells, cell)
		}
		rightmostCell := KeyCell{}.fromBytes(bytes[pg.header.rightmostPtr:])
		pg.cells = append(pg.cells, rightmostCell)
		pg.header.rightmostPtr = pg.header.numOfPtr - 1
	}
	return pg
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

// NonLeafPageではrightmost ptrが有効になるため分割条件が異なる
func (pg *Page) needSplit() bool {
	if pg.header.isLeaf {
		return pg.header.numOfPtr >= MaxDegree
	} else {
		return pg.header.numOfPtr > MaxDegree
	}
}

func (pg *Page) addRecordRec(pageTable *PageTable, rec Record) (splitted bool, splitKey int32, leftPageIndex uint32) {
	key := rec.getKey()
	insert_idx := pg.locateLocally(key)
	if pg.header.isLeaf {
		if len(pg.ptrs) == 0 {
			pg.ptrs = append(pg.ptrs, 0)
		} else {
			pg.ptrs = insertInt(int(insert_idx), uint32(len(pg.cells)), pg.ptrs)
		}
		pg.cells = append(pg.cells, KeyValueCell{key: key, rec: rec})
		pg.header.numOfPtr++
	} else {
		var pageIndex uint32
		if insert_idx == pg.header.numOfPtr {
			pageIndex = pg.cells[pg.header.rightmostPtr].(KeyCell).pageIndex
		} else {
			cellIndex := pg.ptrs[insert_idx]
			pageIndex = pg.cells[cellIndex].(KeyCell).pageIndex
		}
		blk := NewBlockId(pageIndex, StorageFile)

		splitted, splitKey, leftPageIndex := pageTable.pin(blk).addRecordRec(pageTable, rec)
		if splitted {
			if insert_idx == pg.header.numOfPtr {
				// locatelocallyがrightmost ptrを返す時には
				// len(pg.ptr)はpg.header.numOfptr-1になっていることに合わせる
				insert_idx--
			}
			pg.ptrs = insertInt(int(insert_idx), uint32(len(pg.cells)), pg.ptrs)
			pg.cells = append(pg.cells, KeyCell{key: splitKey, pageIndex: leftPageIndex})
			pg.header.numOfPtr++
			pageTable.unpin(NewBlockId(leftPageIndex, StorageFile))
		}
		pageTable.unpin(blk)
	}

	if pg.needSplit() {
		splitted = true
		splitIndex := pg.header.numOfPtr / 2
		if pg.header.isLeaf {
			splitKey = pg.cells[pg.ptrs[splitIndex]].getKey()
		} else {
			splitKey = pg.cells[pg.ptrs[splitIndex-1]].getKey()
		}
		leftPage := newPage(pg.header.isLeaf)
		blk := newUniqueBlockId(StorageFile)
		pageTable.set(blk, leftPage)
		pageTable.pin(blk)
		leftPageIndex = blk.BlockNum
		leftPage.ptrs = make([]uint32, splitIndex)
		leftPage.cells = make([]Cell, splitIndex)
		for i := 0; i < int(splitIndex); i++ {
			leftPage.ptrs[i] = uint32(i)
			leftPage.cells[i] = pg.cells[pg.ptrs[i]]
		}
		leftPage.header.numOfPtr = splitIndex
		if !pg.header.isLeaf {
			leftPage.header.rightmostPtr = splitIndex - 1
		}
		pg.ptrs = pg.ptrs[splitIndex:]
		pg.header.numOfPtr -= splitIndex
	} else {
		splitted = false
	}
	return
}

func (pg *Page) toBytes() []byte {
	buf := make([]byte, PageSize)
	cur := uint32(PageSize)
	var ptrRawValues []uint32
	for _, ptr := range pg.ptrs {
		cell := pg.cells[ptr]
		copy(buf[cur-cell.getSize():cur], cell.toBytes())
		cur = cur - cell.getSize()
		ptrRawValues = append(ptrRawValues, cur)
	}
	for i, value := range ptrRawValues {
		binary.BigEndian.PutUint32(buf[PageHeaderSize+i*IntSize:PageHeaderSize+(i+1)*IntSize], value)
	}

	if pg.header.isLeaf {
		copy(buf[:PageHeaderSize], pg.header.toBytes())
	} else {
		rightmostCell := pg.cells[pg.header.rightmostPtr]
		copy(buf[cur-rightmostCell.getSize():cur], rightmostCell.toBytes())
		cur = cur - rightmostCell.getSize()
		copy(buf[:PageHeaderSize], pg.header.toBytesNonLeaf(cur))
	}
	return buf
}

func (pg *Page) info() {
	fmt.Printf("Page Info {\n")
	fmt.Printf("isLeaf %v\n", pg.header.isLeaf)
	fmt.Printf("numofptrs ... %d\n", pg.header.numOfPtr)
	fmt.Printf("len(ptrs) %d, ptrs... %v\n", len(pg.ptrs), pg.ptrs)
	fmt.Printf("rightmost ptr ... %d\n", pg.header.rightmostPtr)
	fmt.Printf("len(cells) %d, cells... %v\n", len(pg.cells), pg.cells)
	fmt.Printf("}\n")
}
