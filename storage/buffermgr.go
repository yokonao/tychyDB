package storage

import (
	"errors"
	"fmt"
)

const MaxBufferPoolSize = 10

type Buffer struct {
	pin     bool
	ref     bool
	blk     BlockId
	content *Page
}

func newBufferFromPage(blk BlockId, pg *Page) *Buffer {
	buff := &Buffer{}
	buff.content = pg
	buff.pin = false
	buff.ref = false
	buff.blk = blk
	return buff
}

func (buff *Buffer) page() *Page {
	return buff.content
}

func (buff *Buffer) Print() {
	fmt.Printf("Buffer {")
	fmt.Printf("BlockID %d, ", buff.blk)
	if buff.pin {
		fmt.Print("pin, ")
	} else {
		fmt.Print("unpin, ")
	}
	fmt.Printf("ref {%v}", buff.ref)
	fmt.Printf("}")
}

type BufferMgr struct {
	pool []*Buffer
}

func newBufferMgr() *BufferMgr {
	bm := &BufferMgr{}
	bm.pool = make([]*Buffer, MaxBufferPoolSize)
	return bm
}

func (bm *BufferMgr) allocate(buff *Buffer) int {
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bm.pool[i] == nil {
			bm.pool[i] = buff
			return i
		}
	}
	panic(errors.New("no space for page"))
}

func (bm *BufferMgr) load(blk BlockId) int {
	n, bytes := fm.Read(blk)
	if n == 0 {
		panic(errors.New("invalid BlockId was selected"))
	}

	pg := newPageFromBytes(bytes)
	buff := newBufferFromPage(blk, pg)
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bm.pool[i] == nil {
			bm.pool[i] = buff
			return i
		}
	}
	panic(errors.New("no space for page"))
}

func (bm *BufferMgr) Print() {
	fmt.Printf("Print BufferMgr [\n")
	for i, p := range bm.pool {
		if i != 0 {
			fmt.Printf(", ")
		}
		if p == nil {
			fmt.Printf("nil")
		} else {
			p.Print()
		}
		if i != len(bm.pool)-1 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf(" ]\n")
}
