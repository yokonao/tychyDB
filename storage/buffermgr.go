package storage

import (
	"errors"
)

const MaxBufferPoolSize = 5

// The page table keeps track of pages
// that are currently in memory.
// Also maintains additional meta-data per page
type PageTable struct {
	numOfPin int
	table    map[int]int
	queue    Queue
}

func newPageTable() *PageTable {
	ptb := &PageTable{}
	ptb.numOfPin = 0
	ptb.table = make(map[int]int)
	ptb.queue = NewQueue(64)
	return ptb
}

func (ptb *PageTable) makeSpace() {
	if len(ptb.table) > MaxBufferPoolSize {
		panic(errors.New("unexpected"))
	} else if len(ptb.table) < MaxBufferPoolSize {
		return
	}
	if !ptb.available() {
		// unpinされるまで待つ実装でいつか置き換える
		panic(errors.New("unexpected"))
	}
	for {
		dropBlkNum := ptb.queue.Pop()
		dropBlk := newBlockId(uint32(dropBlkNum))
		dropBuffId := ptb.table[int(dropBlkNum)]
		dropBuff := bm.pool[dropBuffId]
		if dropBuff.pin {
			ptb.queue.Push(dropBlkNum)
		} else {
			delete(ptb.table, int(dropBlkNum))
			fm.write(dropBlk, bm.pool[dropBuffId].content)
			bm.pool[dropBuffId] = nil
			break
		}
	}

}

func (ptb *PageTable) getBuffId(blk BlockId) int {
	buffId, exists := ptb.table[int(blk.blockNum)]
	if exists {
		return buffId
	} else {
		ptb.makeSpace()
		ptb.queue.Push(int(blk.blockNum))
		buffId := bm.load(blk)
		ptb.table[int(blk.blockNum)] = buffId
		return buffId
	}
}

func (ptb *PageTable) available() bool {
	if ptb.numOfPin == MaxBufferPoolSize {
		return false
	} else {
		return true
	}
}

func (ptb *PageTable) set(blk BlockId, pg *Page) {
	ptb.makeSpace()
	ptb.queue.Push(int(blk.blockNum))
	buff := newBufferFromPage(pg)
	buffId := bm.allocate(buff)
	ptb.table[int(blk.blockNum)] = buffId
}

func (ptb *PageTable) read(blk BlockId) *Page {
	return bm.pool[ptb.getBuffId(blk)].content
}

func (ptb *PageTable) pin(blk BlockId) *Page {
	buff := bm.pool[ptb.getBuffId(blk)]
	buff.pin = true
	ptb.numOfPin++
	return buff.content
}

func (ptb *PageTable) unpin(blk BlockId) {
	buffId, exists := ptb.table[int(blk.blockNum)]
	if !exists {
		panic(errors.New("trying to unpin page not on disk"))
	}
	buff := bm.pool[buffId]
	if !buff.pin {
		panic(errors.New("pin is already unpinned"))
	}
	buff.pin = false
	ptb.numOfPin--
}

type Buffer struct {
	pin     bool
	content *Page
}

func newBufferFromPage(pg *Page) *Buffer {
	buff := &Buffer{}
	buff.content = pg
	buff.pin = false
	return buff
}

func (buff *Buffer) page() *Page {
	return buff.content
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
	n, pg := fm.read(blk)
	if n == 0 {
		panic(errors.New("invalid BlockId was selected"))
	}
	buff := newBufferFromPage(pg)
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bm.pool[i] == nil {
			bm.pool[i] = buff
			return i
		}
	}
	panic(errors.New("no space for page"))
}
