package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Type struct {
	name string
	size uint
}

func (t Type) String() string {
	return t.name
}

type Column struct {
	ty   Type
	name string
	pos  uint
}

func (c Column) String() string {
	return fmt.Sprintf("{ type: %s, name: %s }", c.ty, c.name)
}

type Record struct {
	data []byte
}

type Table struct {
	cols  []Column
	pages []Page
}

func (t *Table) getRecordSize() uint {
	last := t.cols[len(t.cols)-1]
	return last.pos + last.ty.size
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

func (t *Table) AddColumn(name string) {
	var pos uint
	if len(t.cols) == 0 {
		pos = 0
	} else {
		last := t.cols[len(t.cols)-1]
		pos = last.pos + last.ty.size
	}
	ty := Type{name: "int", size: 4}
	t.cols = append(t.cols, Column{ty: ty, name: name, pos: pos})
	// fmt.Println(s.cols[len(s.cols)-1])
}

func (t *Table) addRecord(rec Record) {
	for i := 0; ; i++ {
		if len(t.pages) <= i {
			t.pages = append(t.pages, newPage())
		}

		res := t.pages[i].setBytes(rec.data)
		if res {
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
		if col.ty.name == "int" {
			val := uint32(args[i].(int))
			buf := make([]byte, col.ty.size)
			binary.BigEndian.PutUint32(buf, val)
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
	t.addRecord(Record{data: bytes})
	return nil
}

func (t *Table) selectInt(col Column) (res []int32, err error) {
	if col.ty.name != "int" {
		return nil, errors.New("you must specify int type column")
	}
	for _, pg := range t.pages {
		if !pg.header.isLeaf {
			continue
		}
		numOfPtr := pg.header.numOfPtr
		for i := 0; uint32(i) < numOfPtr; i++ {
			sz := t.getRecordSize()
			bytes := pg.bb[sz*uint(i)+col.pos : sz*uint(i)+col.pos+col.ty.size]
			res = append(res, int32(binary.BigEndian.Uint32(bytes)))
		}
	}
	return
}

func (t *Table) Select(names ...string) (res [][]int32, err error) {
	for _, name := range names {
		for _, col := range t.cols {
			if name != col.name {
				continue
			}
			if col.ty.name == "int" {
				values, err := t.selectInt(col)
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
