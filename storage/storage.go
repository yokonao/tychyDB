package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/tychyDB/algorithm"
)

const StorageFile = "storage"

func ResetBlockId() {
	UniqueBlockId = 0
}

type Storage struct {
	fm  *FileMgr
	ptb *PageTable
	MetaPage
}

func NewStorage(fm *FileMgr, ptb *PageTable) Storage {
	st := Storage{}
	// テーブルのメタ情報を置くためのページ

	metaBlk := newUniqueBlockId(StorageFile)
	st.metaBlk = metaBlk
	if metaBlk.BlockNum != 0 {
		panic(errors.New("place a meta page at the top of the file"))
	}
	st.fm = fm
	st.fm.Write(metaBlk, st.MetaPage.toBytes())

	st.ptb = ptb
	// rootノード
	root := newPage(false)
	st.rootBlk = newUniqueBlockId(StorageFile)
	ptb.set(st.rootBlk, root)
	st.cols = []Column{}
	return st
}

func NewStorageFromFile(fm *FileMgr, ptb *PageTable) Storage {
	st := Storage{}
	blk := newUniqueBlockId(StorageFile)
	if blk.BlockNum != 0 {
		panic(errors.New("expect 0"))
	}
	st.fm = fm
	st.ptb = ptb
	_, bytes := fm.Read(blk)
	st.MetaPage = newMetaPageFromBytes(bytes)
	return st
}

func (st *Storage) Flush() {
	st.ptb.Flush()
	fmt.Println("filename -------- ", st.metaBlk.fileName)
	st.fm.Write(st.metaBlk, st.MetaPage.toBytes())
}

func (st *Storage) addRecord(rec Record) {
	rootPage := st.ptb.pin(st.rootBlk)
	if rootPage.header.numOfPtr == 0 {
		pg := newPage(true)
		blk := newUniqueBlockId(StorageFile)
		st.ptb.set(blk, pg)
		rootPage.cells = append(rootPage.cells, KeyCell{key: math.MaxInt32, pageIndex: blk.BlockNum})
		rootPage.header.rightmostPtr = 0
		rootPage.header.numOfPtr++
		pg.ptrs = append(pg.ptrs, 0)
		pg.cells = append(pg.cells, KeyValueCell{key: rec.getKey(), rec: rec})
		pg.header.numOfPtr++
		st.ptb.unpin(st.rootBlk)
	} else {
		splitted, splitKey, leftPageIndex := rootPage.addRecordRec(st.ptb, rec)
		if splitted {
			newRootPage := newPage(false)
			blk := newUniqueBlockId(StorageFile)
			st.ptb.set(blk, newRootPage)
			st.ptb.pin(blk)
			newRootPage.header.rightmostPtr = 0
			newRootPage.ptrs = append(newRootPage.ptrs, 1)
			newRootPage.cells = append(newRootPage.cells, KeyCell{key: math.MaxInt32, pageIndex: st.rootBlk.BlockNum})
			newRootPage.cells = append(newRootPage.cells, KeyCell{key: splitKey, pageIndex: leftPageIndex})
			newRootPage.header.numOfPtr += 2
			st.rootBlk = blk
		}
		st.ptb.unpin(st.rootBlk)
	}
}

func (st *Storage) AddColumn(name string, ty Type) {
	var pos uint32
	if st.ColumnLength() == 0 {
		pos = 0
	} else {
		last := st.cols[st.ColumnLength()-1]
		pos = last.pos + last.ty.size
	}
	st.cols = append(st.cols, Column{ty: ty, name: name, pos: pos})
}

func (st *Storage) Add(args ...interface{}) error {
	bytes, err := encode(st.cols, args...)
	if err != nil {
		return err
	}
	st.addRecord(Record{size: uint32(len(bytes)), data: bytes})
	return nil
}

func (st *Storage) Update(prVal interface{}, targetColName string, replaceTo interface{}) UpdateInfo {
	col, _ := st.GetPrColumn()
	prKey := st.GetPrimaryKey(prVal)
	curBlk := st.SearchPrKey(prKey)
	curPage := st.ptb.pin(curBlk)
	// レコードの書き換え
	// 対象のカラムを検索
	targetColIndex := -1
	for i, c := range st.cols {
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

	targetCol := st.cols[targetColIndex]

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
	st.ptb.unpin(curBlk)
	// UpdateInfoの作成
	updateInfo := NewUpdateInfo(curBlk.BlockNum, ptrIdx, uint32(targetColIndex), fromBuf, toBuf)
	return updateInfo
}

func (st *Storage) selectInt(col Column) (res []interface{}, err error) {
	if col.ty.id != integerId {
		return nil, errors.New("you must specify int type column")
	}
	pageQueue := algorithm.NewQueue(64)
	pageQueue.Push(int(st.rootBlk.BlockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := st.ptb.read(NewBlockId(curPageIndex, StorageFile))
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

func (st *Storage) selectChar(col Column) (res []interface{}, err error) {

	if col.ty.id != charId {
		return nil, errors.New("you must specify char type column")
	}
	pageQueue := algorithm.NewQueue(64)
	pageQueue.Push(int(st.rootBlk.BlockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := st.ptb.read(NewBlockId(curPageIndex, StorageFile))
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

func (st *Storage) Select(names ...string) (res [][]interface{}, err error) {
	for _, name := range names {
		for _, col := range st.cols {
			if name != col.name {
				continue
			}
			if col.ty.id == integerId {
				values, err := st.selectInt(col)
				if err != nil {
					return nil, err
				}
				res = append(res, values)
				break
			} else if col.ty.id == charId {
				values, err := st.selectChar(col)
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

func (st *Storage) Print() {
	fmt.Println("--- start table print ---")
	pageQueue := algorithm.NewQueue(64)
	pageQueue.Push(int(st.rootBlk.BlockNum))
	for !pageQueue.IsEmpty() {
		curPageIndex := uint32(pageQueue.Pop())
		curPage := st.ptb.read(NewBlockId(curPageIndex, StorageFile))
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

func (st *Storage) ColumnLength() int {
	return len(st.cols)
}

func (st *Storage) GetPrColumn() (Column, error) {
	if st.ColumnLength() == 0 {
		return Column{}, errors.New("out of range")
	}
	// use index 0 as primary column for now
	return (st.cols)[0], nil
}

func (st *Storage) GetPrimaryKey(prVal interface{}) int32 {
	col, _ := st.GetPrColumn()
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
	return int32(binary.BigEndian.Uint32(buf[:IntSize]))
}

func (st *Storage) SearchPrKey(prKey int32) BlockId {
	rootPage := st.ptb.pin(st.rootBlk)
	if rootPage.header.numOfPtr == 0 {
		panic(errors.New("unexpected"))
	}
	curBlk := st.rootBlk
	curPage := rootPage
	for !curPage.header.isLeaf {
		idx := curPage.locateLocally(prKey)
		var childBlkId uint32
		if idx == curPage.header.numOfPtr {
			childBlkId = curPage.cells[curPage.header.rightmostPtr].(KeyCell).pageIndex
		} else {
			childBlkId = curPage.cells[curPage.ptrs[idx]].(KeyCell).pageIndex
		}
		childBlk := NewBlockId(childBlkId, StorageFile)
		childPage := st.ptb.pin(childBlk)
		st.ptb.unpin(curBlk)
		curBlk = childBlk
		curPage = childPage
	}
	st.ptb.unpin(curBlk)
	return curBlk
}
