package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

var fm = NewFileMgr("testfile")
var bm = newBufferMgr()
var ptb = newPageTable()

func Clean() {
	fm.Clean()
}

func Reset() {
	UniqueBlockId = 0
	bm = newBufferMgr()
	ptb = newPageTable()
}

type Column struct {
	ty   Type
	pos  uint32
	name string
}

func (c Column) String() string {
	return fmt.Sprintf("{ type: %s, name: %s }", c.ty, c.name)
}

func (c Column) toBytes() []byte {
	bytes := []byte{}
	buf := make([]byte, 4*IntSize)
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(4*IntSize+c.ty.size))
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], uint32(c.ty.id))
	binary.BigEndian.PutUint32(buf[2*IntSize:3*IntSize], c.ty.size)
	binary.BigEndian.PutUint32(buf[3*IntSize:4*IntSize], c.pos)

	bytes = append(bytes, buf...)

	buf = make([]byte, c.ty.size)
	rd := strings.NewReader(c.name)
	rd.Read(buf)
	bytes = append(bytes, buf...)
	return bytes
}

func newColumnfromBytes(bytes []byte) Column {
	c := Column{}
	c.ty.id = TypeId(binary.BigEndian.Uint32(bytes[IntSize : 2*IntSize]))
	c.ty.size = binary.BigEndian.Uint32(bytes[2*IntSize : 3*IntSize])
	c.pos = binary.BigEndian.Uint32(bytes[3*IntSize : 4*IntSize])
	c.name = string(bytes[4*IntSize:])
	return c
}

type Table struct {
	cols     []Column
	rootBlk  BlockId
	metaPage *MetaPage
}

func NewTable() Table {
	t := Table{}
	// テーブルのメタ情報を置くためのページ

	metaBlk := newUniqueBlockId()
	t.metaPage = newMetaPage(metaBlk)
	if metaBlk.blockNum != 0 {
		panic(errors.New("place a meta page at the top of the file"))
	}
	fm.Write(metaBlk, t.metaPage.toBytes())

	// rootノード
	root := newPage(false)
	t.rootBlk = newUniqueBlockId()
	ptb.set(t.rootBlk, root)
	t.metaPage.rootBlk = t.rootBlk
	return t
}

func NewTableFromFIle() Table {
	t := Table{}
	blk := newUniqueBlockId()
	if blk.blockNum != 0 {
		panic(errors.New("expect 0"))
	}
	_, bytes := fm.Read(blk)
	t.metaPage = newMetaPageFromBytes(bytes)
	t.rootBlk = t.metaPage.rootBlk
	t.cols = t.metaPage.cols
	return t
}

func (t *Table) Flush() {
	ptb.flush()
	fm.Write(t.metaPage.metaBlk, t.metaPage.toBytes())
}

func (t *Table) AddColumn(name string, ty Type) {
	var pos uint32
	if len(t.cols) == 0 {
		pos = 0
	} else {
		last := t.cols[len(t.cols)-1]
		pos = last.pos + last.ty.size
	}
	t.cols = append(t.cols, Column{ty: ty, name: name, pos: pos})
	t.metaPage.cols = append(t.cols, Column{ty: ty, name: name, pos: pos})
}

func (t *Table) addRecord(rec Record) {
	rootPage := ptb.pin(t.rootBlk)
	if rootPage.header.numOfPtr == 0 {
		pg := newPage(true)
		blk := newUniqueBlockId()
		ptb.set(blk, pg)
		rootPage.cells = append(rootPage.cells, KeyCell{key: math.MaxInt32, pageIndex: blk.blockNum})
		rootPage.header.rightmostPtr = 0
		rootPage.header.numOfPtr++
		pg.ptrs = append(pg.ptrs, 0)
		pg.cells = append(pg.cells, KeyValueCell{key: rec.getKey(), rec: rec})
		pg.header.numOfPtr++
		ptb.unpin(t.rootBlk)
	} else {
		splitted, splitKey, leftPageIndex := rootPage.addRecordRec(rec)
		if splitted {
			newRootPage := newPage(false)
			blk := newUniqueBlockId()
			ptb.set(blk, newRootPage)
			ptb.pin(blk)
			newRootPage.header.rightmostPtr = 0
			newRootPage.ptrs = append(newRootPage.ptrs, 1)
			newRootPage.cells = append(newRootPage.cells, KeyCell{key: math.MaxInt32, pageIndex: t.rootBlk.blockNum})
			newRootPage.cells = append(newRootPage.cells, KeyCell{key: splitKey, pageIndex: leftPageIndex})
			newRootPage.header.numOfPtr += 2
			t.rootBlk = blk
			t.metaPage.rootBlk = blk
		}
		ptb.unpin(t.rootBlk)
	}
}

func encode(cols []Column, args ...interface{}) (bytes []byte, err error) {
	if len(args) != len(cols) {
		err = errors.New("the count of arguments must be same column's")
		return
	}
	bytes = []byte{}
	for i, col := range cols {
		if col.ty.id == integerId {
			val := uint32(args[i].(int))
			buf := make([]byte, col.ty.size)
			binary.BigEndian.PutUint32(buf, val)
			bytes = append(bytes, buf...)
		} else if col.ty.id == charId {
			rd := strings.NewReader(args[i].(string))
			buf := make([]byte, col.ty.size)
			rd.Read(buf)
			bytes = append(bytes, buf...)
		} else {
			bytes = nil
			err = errors.New("the type of a column is not implemented")
			return
		}
	}
	return
}

func (t *Table) Add(args ...interface{}) error {
	bytes, err := encode(t.cols, args...)
	if err != nil {
		return err
	}
	t.addRecord(Record{size: uint32(len(bytes)), data: bytes})
	return nil
}

func (t *Table) Update(prColName string, prVal interface{}, targetColName string, replaceTo interface{}) transaction.UpdateInfo {
	rootPage := ptb.pin(t.rootBlk)
	if rootPage.header.numOfPtr == 0 {
		panic(errors.New("unexpected"))
	}

	//　該当レコードを検索
	if prColName != t.cols[0].name {
		panic(errors.New("input col is nor primary"))
	}

	// キーの計算
	col := t.cols[0]
	buf := make([]byte, col.ty.size)
	if col.ty.id == integerId {
		val := uint32(prVal.(int))
		binary.BigEndian.PutUint32(buf, val)
	} else if col.ty.id == charId {
		rd := strings.NewReader(prVal.(string))
		rd.Read(buf)
	} else {
		panic(errors.New("the type of a column is not implemented"))
	}
	prKey := int32(binary.BigEndian.Uint32(buf[:IntSize]))
	curBlk := t.rootBlk
	curPage := rootPage
	for !curPage.header.isLeaf {
		idx := curPage.locateLocally(prKey)
		var childBlkId uint32
		if idx == curPage.header.numOfPtr {
			childBlkId = curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex
		} else {
			childBlkId = curPage.cells[curPage.ptrs[idx]].(KeyCell).pageIndex
		}
		childBlk := newBlockId(childBlkId)
		childPage := ptb.pin(childBlk)
		ptb.unpin(curBlk)
		curBlk = childBlk
		curPage = childPage
	}

	// レコードの書き換え
	// 対象のカラムを検索
	targetColIndex := -1
	for i, c := range t.cols {
		if c.name == targetColName {
			targetColIndex = i
		}
	}
	if targetColIndex == 0 {
		panic(errors.New("cannot update primary key"))
	}
	if targetColIndex == -1 {
		panic(errors.New("invalid target column name"))
	}

	targetCol := t.cols[targetColIndex]

	// 該当レコードを取得
	ptrIdx := curPage.locateLocally(prKey)
	if ptrIdx == 0 {
		// locate locallyにおいてleaf Nodeの倍に key <= comparedにすればここはいらなくなる?
		// ただしページ分割なども修正する必要あり
		panic(errors.New("key not found"))
	}
	cellIdx := curPage.ptrs[ptrIdx-1]
	if curPage.cells[cellIdx].getKey() != prKey {
		panic(errors.New("key not found"))
	}
	// レコードを抜き出す
	rec := curPage.cells[cellIdx].(KeyValueCell).rec
	fromBuf := make([]byte, col.ty.size)
	toBuf := make([]byte, col.ty.size)
	if targetCol.ty.id == integerId {
		val := uint32(replaceTo.(int))
		binary.BigEndian.PutUint32(toBuf, val)
	} else if targetCol.ty.id == charId {
		rd := strings.NewReader(replaceTo.(string))
		rd.Read(toBuf)
	}
	copy(fromBuf, rec.data[targetCol.pos:targetCol.pos+targetCol.ty.size])
	copy(rec.data[targetCol.pos:targetCol.pos+targetCol.ty.size], toBuf)

	curPage.cells[cellIdx] = KeyValueCell{key: rec.getKey(), rec: rec}

	// UpdateInfoの作成
	updateInfo := transaction.NewUpdateInfo(curBlk.blockNum, ptrIdx, uint32(targetColIndex), fromBuf, toBuf)
	return updateInfo
}

func (t *Table) selectInt(col Column) (res []interface{}, err error) {
	if col.ty.id != integerId {
		return nil, errors.New("you must specify int type column")
	}
	pageQueue := NewQueue(64)
	pageQueue.Push(int(t.rootBlk.blockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := ptb.read(newBlockId(curPageIndex))
		if curPage.header.isLeaf {
			for _, ptr := range curPage.ptrs {
				rec := curPage.cells[ptr].(KeyValueCell).rec
				bytes := rec.data[col.pos : col.pos+col.ty.size]
				res = append(res, int32(binary.BigEndian.Uint32(bytes)))
			}
		} else {
			for i := 0; i < int(curPage.header.numOfPtr-1); i++ {
				pageQueue.Push(int(curPage.cells[curPage.ptrs[i]].(KeyCell).pageIndex))
			}
			pageQueue.Push(int(curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex))
		}
	}
	return
}

func (t *Table) selectChar(col Column) (res []interface{}, err error) {

	if col.ty.id != charId {
		return nil, errors.New("you must specify int type column")
	}
	pageQueue := NewQueue(64)
	pageQueue.Push(int(t.rootBlk.blockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := ptb.read(newBlockId(curPageIndex))
		if curPage.header.isLeaf {

			for _, ptr := range curPage.ptrs {
				rec := curPage.cells[ptr].(KeyValueCell).rec
				bytes := rec.data[col.pos : col.pos+col.ty.size]
				res = append(res, string(bytes))
			}
		} else {
			for i := 0; i < int(curPage.header.numOfPtr-1); i++ {
				pageQueue.Push(int(curPage.cells[curPage.ptrs[i]].(KeyCell).pageIndex))
			}
			pageQueue.Push(int(curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex))
		}

	}
	return
}

func (t *Table) Select(names ...string) (res [][]interface{}, err error) {
	for _, name := range names {
		for _, col := range t.cols {
			if name != col.name {
				continue
			}
			if col.ty.id == integerId {
				values, err := t.selectInt(col)
				if err != nil {
					return nil, err
				}
				res = append(res, values)
				break
			} else if col.ty.id == charId {
				values, err := t.selectChar(col)
				if err != nil {
					return nil, err
				}
				res = append(res, values)
				break
			} else {
				return nil, errors.New("the type of a column is not implemented")
			}
		}
	}
	for _, name := range names {
		fmt.Printf("| %s\t", name)
	}
	fmt.Print("|")
	fmt.Print("\n")
	for i := 0; i < len(names); i++ {
		fmt.Printf("| --\t")
	}
	fmt.Print("|")
	fmt.Print("\n")
	for i := 0; i < len(res[0]); i++ {
		for j := 0; j < len(names); j++ {
			fmt.Printf("| %v\t", res[j][i])
		}
		fmt.Print("|")
		fmt.Print("\n")
	}
	return res, nil
}

func (t *Table) Print() {
	fmt.Println("--- start table print ---")
	pageQueue := NewQueue(64)
	pageQueue.Push(int(t.rootBlk.blockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := ptb.read(newBlockId(curPageIndex))
		fmt.Printf("Page Index is %d\n", curPageIndex)
		curPage.info()
		if !curPage.header.isLeaf {
			for i := 0; i < int(curPage.header.numOfPtr-1); i++ {
				pageQueue.Push(int(curPage.cells[curPage.ptrs[i]].(KeyCell).pageIndex))
			}
			pageQueue.Push(int(curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex))
		}

	}

}
