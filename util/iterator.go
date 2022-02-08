package util

import (
	"encoding/binary"
	"errors"
)

type IterStruct struct {
	cur     uint32
	bytes   []byte
	byteLen uint32
}

func NewIterStruct(top uint32, bytes []byte) *IterStruct {
	is := &IterStruct{}
	is.cur = top
	is.byteLen = uint32(len(bytes))
	is.bytes = make([]byte, is.byteLen)
	copy(is.bytes, bytes)
	return is
}

func (is *IterStruct) NextUInt32() uint32 {
	if is.cur+IntSize > is.byteLen {
		panic(errors.New("bytes end"))
	}
	res := binary.BigEndian.Uint32(is.bytes[is.cur : is.cur+IntSize])
	is.cur += IntSize
	return res
}

func (is *IterStruct) NextBool() bool {
	if is.cur+BoolSize > is.byteLen {
		panic(errors.New("bytes end"))
	}
	var res bool
	if is.bytes[is.cur] == 0 {
		res = false
	} else {
		res = true
	}
	is.cur += BoolSize
	return res
}

func (is *IterStruct) NextBytes(n uint32) []byte {
	if is.cur+n > is.byteLen {
		panic(errors.New("bytes end"))
	}
	res := make([]byte, n)
	copy(res, is.bytes[is.cur:is.cur+n])
	is.cur += n
	return res
}

func (is *IterStruct) NextUint32WithSize() uint32 {
	if size := is.NextUInt32(); size != 4 {
		panic(errors.New("expect size 4"))
	}
	return is.NextUInt32()
}

func (is *IterStruct) NextStringWithSize(cap uint32) string {
	size := is.NextUInt32()
	s := string(is.NextBytes(cap))
	return s[:size]
}
