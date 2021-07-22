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

type Storage struct {
	cols []Column
	data []Record
}

func NewStorage() Storage {
	return Storage{}
}

func (s *Storage) AddColumn(name string) {
	var pos uint
	if len(s.cols) == 0 {
		pos = 0
	} else {
		last := s.cols[len(s.cols)-1]
		pos = last.pos + last.ty.size
	}
	ty := Type{name: "int", size: 4}
	s.cols = append(s.cols, Column{ty: ty, name: name, pos: pos})
	// fmt.Println(s.cols[len(s.cols)-1])
}

func (s *Storage) addRecord(rec Record) {
	s.data = append(s.data, rec)
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

func (s *Storage) Add(args ...interface{}) error {
	bytes, err := encode(s.cols, args...)
	if err != nil {
		return err
	}
	s.addRecord(Record{data: bytes})
	return nil
}

func (s *Storage) selectInt(col Column) (res []int32, err error) {
	if col.ty.name != "int" {
		return nil, errors.New("you must specify int type column")
	}
	for _, rec := range s.data {
		bytes := rec.data[col.pos : col.pos+col.ty.size]
		res = append(res, int32(binary.LittleEndian.Uint32(bytes)))
	}
	return
}

func (s *Storage) Select(names ...string) (res [][]int32, err error) {
	for _, name := range names {
		for _, col := range s.cols {
			if name != col.name {
				continue
			}
			if col.ty.name == "int" {
				values, err := s.selectInt(col)
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
