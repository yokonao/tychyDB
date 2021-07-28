package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

type Column struct {
	ty   Type
	name string
	pos  uint32
}

func (c Column) String() string {
	return fmt.Sprintf("{ type: %s, name: %s }", c.ty, c.name)
}

type Table struct {
	cols  []Column
	pages []Page
}

func NewTable() Table {
	t := Table{}
	t.pages = append(t.pages, newNonLeafPage())
	return t
}

func (t *Table) Write() {
	fm := newFileMgr()
	for i, pg := range t.pages {
		blk := newBlockId("testfile", int64(i))
		fm.write(blk, &pg)
	}
}

func (t *Table) Read() {
	fm := newFileMgr()

	t.pages = make([]Page, 0)
	for i := 0; ; i++ {
		blk := newBlockId("testfile", int64(i))
		n, pg := fm.read(blk)
		if n == 0 {
			break
		}

		t.pages = append(t.pages, pg)
	}
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
	// fmt.Println(s.cols[len(s.cols)-1])
}

func (t *Table) addRecord(rec Record) {
	for i := 0; ; i++ {
		if len(t.pages) <= i {
			t.pages = append(t.pages, newPage())
		}

		res, index := t.pages[i].addRecord(rec)
		if res {
			t.pages[0].addKeyCell(KeyCell{key: rec.getKey(), pageIndex: uint32(i), ptrIndex: index})
			break
		}
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

func (t *Table) selectInt(col Column) (res []interface{}, err error) {
	if col.ty.id != integerId {
		return nil, errors.New("you must specify int type column")
	}
	root := t.pages[0]
	for i := 0; i < int(root.header.numOfPtr); i++ {
		idx := root.ptrs[i]
		keyCell := root.cells[idx].(KeyCell)
		rec := t.pages[keyCell.pageIndex].cells[keyCell.ptrIndex].(Record)
		bytes := rec.data[col.pos : col.pos+col.ty.size]
		res = append(res, int32(binary.BigEndian.Uint32(bytes)))
	}
	return
}

func (t *Table) selectChar(col Column) (res []interface{}, err error) {
	if col.ty.id != charId {
		return nil, errors.New("you must specify int type column")
	}
	root := t.pages[0]
	for i := 0; i < int(root.header.numOfPtr); i++ {
		idx := root.ptrs[i]
		keyCell := root.cells[idx].(KeyCell)
		rec := t.pages[keyCell.pageIndex].cells[keyCell.ptrIndex].(Record)
		bytes := rec.data[col.pos : col.pos+col.ty.size]
		res = append(res, string(bytes))
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
			fmt.Printf("| %d\t", res[j][i])
		}
		fmt.Print("|")
		fmt.Print("\n")
	}
	return res, nil
}
