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
	fmt.Printf("BlockID %d, ", buff.blk.BlockNum)
	if buff.pin {
		fmt.Print("pin, ")
	} else {
		fmt.Print("unpin, ")
	}
	fmt.Printf("ref {%v}", buff.ref)
	fmt.Printf("}")
}

type BufferMgr struct {
	fileManager *FileMgr
	pool        []*Buffer
}

func NewBufferMgr(fileManager *FileMgr) *BufferMgr {
	bufferManager := &BufferMgr{}
	bufferManager.fileManager = fileManager
	bufferManager.pool = make([]*Buffer, MaxBufferPoolSize)
	return bufferManager
}

func (bufferManager *BufferMgr) pageAt(buffId int) *Page {
	return bufferManager.pool[buffId].page()
}

func (bufferManager *BufferMgr) allocate(buff *Buffer) int {
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bufferManager.pool[i] == nil {
			bufferManager.pool[i] = buff
			return i
		}
	}
	panic(errors.New("no space for page"))
}

func (bufferManager *BufferMgr) load(blk BlockId) int {
	n, bytes := bufferManager.fileManager.Read(blk)
	if n == 0 {
		panic(errors.New("invalid BlockId was selected"))
	}

	pg := newPageFromBytes(bytes)
	buff := newBufferFromPage(blk, pg)
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bufferManager.pool[i] == nil {
			bufferManager.pool[i] = buff
			return i
		}
	}
	panic(errors.New("no space for page"))
}

func (bufferManager *BufferMgr) flush(buffId int) {
	buff := bufferManager.pool[buffId]
	bufferManager.fileManager.Write(buff.blk, buff.page().toBytes())
	bufferManager.pool[buffId] = nil
}

func (bufferManager *BufferMgr) isPinned(buffId int) bool {
	buff := bufferManager.pool[buffId]
	return buff.pin
}

func (bufferManager *BufferMgr) pin(buffId int) {
	buff := bufferManager.pool[buffId]
	buff.pin = true
	buff.ref = true
}

func (bufferManager *BufferMgr) unpin(buffId int) {
	buff := bufferManager.pool[buffId]
	if !buff.pin {
		panic(errors.New("pin is already unpinned"))
	}
	buff.pin = false
}

func (bufferManager *BufferMgr) isRefed(buffId int) bool {
	buff := bufferManager.pool[buffId]
	return buff.ref
}

func (bufferManager *BufferMgr) unRef(buffId int) {
	buff := bufferManager.pool[buffId]
	buff.ref = false
}

func (bufferManager *BufferMgr) clear(buffId int) {
	bufferManager.pool[buffId] = nil
}

func (bufferManager *BufferMgr) Print() {
	fmt.Printf("Print BufferMgr [\n")
	for i, p := range bufferManager.pool {
		if i != 0 {
			fmt.Printf(", ")
		}
		if p == nil {
			fmt.Printf("nil")
		} else {
			p.Print()
		}
		if i != len(bufferManager.pool)-1 {
			fmt.Printf("\n")
		}
	}
	fmt.Printf(" ]\n")
}
