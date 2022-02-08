package util

import (
	"encoding/binary"
	"errors"
	"strings"
)

type GenStruct struct {
	cur     uint32
	bytes   []byte
	byteLen uint32
}

func NewGenStruct(top uint32, byteLen uint32) *GenStruct {
	gs := &GenStruct{}
	gs.cur = top
	gs.byteLen = byteLen
	gs.bytes = make([]byte, byteLen)
	return gs
}

func (gs *GenStruct) DumpBytes() []byte {
	return gs.bytes
}

func (gs *GenStruct) PutUInt32(val uint32) {
	if gs.cur+IntSize > gs.byteLen {
		panic(errors.New("buffer is full"))
	}
	binary.BigEndian.PutUint32(gs.bytes[gs.cur:gs.cur+IntSize], val)
	gs.cur += IntSize
}

func (gs *GenStruct) PutBool(flag bool) {
	if gs.cur+IntSize > gs.byteLen {
		panic(errors.New("buffer is full"))
	}

	if flag {
		gs.bytes[gs.cur] = 1
	} else {
		gs.bytes[gs.cur] = 0
	}
	gs.cur += BoolSize
}

func (gs *GenStruct) PutBytes(n uint32, bytes []byte) {
	if gs.cur+n > gs.byteLen {
		panic(errors.New("buffer is full"))
	}
	copy(gs.bytes[gs.cur:gs.cur+n], bytes)
	gs.cur += n
}

func (gs *GenStruct) PutUInt32WithSize(val uint32) {
	// todo add validation
	gs.PutUInt32(4)
	gs.PutUInt32(val)
}

func (gs *GenStruct) PutStringWithSize(s string, cap uint32) {
	// todo add validation
	gs.PutUInt32(uint32(len(s)))
	buf := make([]byte, cap)
	rd := strings.NewReader(s)
	rd.Read(buf)
	gs.PutBytes(cap, buf)
}
