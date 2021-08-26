package storage

import (
	"errors"
)

const MaxBufferPoolSize = 5

type PageTable struct {
	table map[int]int
	queue []BlockId
}

func newPageTable() *PageTable {
	ptb := &PageTable{}
	ptb.table = make(map[int]int)
	ptb.queue = make([]BlockId, 0)
	return ptb
}

func (ptb *PageTable) makeSpace() {
	if len(ptb.table) > MaxBufferPoolSize {
		panic(errors.New("unexpected"))
	} else if len(ptb.table) < MaxBufferPoolSize {
		return
	}

	dropBlk := ptb.queue[0]
	ptb.queue = ptb.queue[1:]

	dropBuffId := ptb.table[int(dropBlk.blockNum)]
	delete(ptb.table, int(dropBlk.blockNum))
	fm.write(dropBlk, bm.pool[dropBuffId])
	bm.pool[dropBuffId] = nil
}

func (ptb *PageTable) getBuffId(blk BlockId) int {
	buffId, exists := ptb.table[int(blk.blockNum)]
	if exists {
		return buffId
	} else {
		ptb.makeSpace()
		ptb.queue = append(ptb.queue, blk)
		buffId := bm.load(blk)
		ptb.table[int(blk.blockNum)] = buffId
		return buffId
	}
}

func (ptb *PageTable) set(blk BlockId, pg *Page) {
	ptb.makeSpace()
	ptb.queue = append(ptb.queue, blk)
	buffId := bm.allocate(pg)
	ptb.table[int(blk.blockNum)] = buffId
}

type BufferMgr struct {
	pool []*Page
}

func newBufferMgr() *BufferMgr {
	bm := &BufferMgr{}
	bm.pool = make([]*Page, MaxBufferPoolSize)
	return bm
}

func (bm *BufferMgr) allocate(pg *Page) int {
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bm.pool[i] == nil {
			bm.pool[i] = pg
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
	for i := 0; i < MaxBufferPoolSize; i++ {
		if bm.pool[i] == nil {
			bm.pool[i] = pg
			return i
		}
	}
	panic(errors.New("no space for page"))
}
