package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
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
	data  []Record
	pages []Page
}

func (t *Table) getRecordSize() uint {
	last := t.cols[len(t.cols)-1]
	return last.pos + last.ty.size
}

func NewTable() Table {
	return Table{}
}

func (t *Table) Write() {
	if len(t.pages) == 0 {
		t.pages = append(t.pages, newPage())
	}
	for _, rec := range t.data {
		t.pages[0].setBytes(rec.data)
	}
	file, err := os.Create("testfile")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write(t.pages[0].bb)
}

func (t *Table) Read() {
	file, err := os.Open("testfile")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	buf := make([]byte, PageSize)
	file.Read(buf)
	if len(t.pages) == 0 {
		t.pages = append(t.pages, newPage())
	}
	t.pages[0].setBytes(buf)
	t.data = make([]Record, 0)
	sz := int(t.getRecordSize())
	for i := 0; i < sz; i++ {
		t.data = append(t.data, Record{data: buf[i*sz : (i+1)*sz]})
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
	t.data = append(t.data, rec)
	// fmt.Println(rec.data)
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
			binary.LittleEndian.PutUint32(buf, val)
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
	for _, rec := range t.data {
		bytes := rec.data[col.pos : col.pos+col.ty.size]
		res = append(res, int32(binary.LittleEndian.Uint32(bytes)))
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
