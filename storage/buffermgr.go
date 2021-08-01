package storage

import (
	"errors"
)

type BufferMgr struct {
	bufferPool []Buffer
}

func newBufferMgr() BufferMgr {
	return BufferMgr{}
}

func (bm *BufferMgr) clear() {
	bm.bufferPool = make([]Buffer, 0)
}

func (bm *BufferMgr) size() int {
	return len(bm.bufferPool)
}

func (bm *BufferMgr) append(fm FileMgr, blk BlockId, pg Page) {
	buff := newBuffer(fm, blk, pg)
	bm.bufferPool = append(bm.bufferPool, buff)
}

func (bm *BufferMgr) findBuffer(blk BlockId) (bool, int) {
	for i, buff := range bm.bufferPool {
		b := buff.blk
		if b.equal(blk) {
			return true, i
		}
	}
	return false, 0
}

func (bm *BufferMgr) addRecord(rec Record, blk BlockId) (res bool, index uint32) {
	exists, n := bm.findBuffer(blk)
	if !exists {
		panic(errors.New("invalid blockId access occured"))
	}
	res, index = bm.bufferPool[n].pg.addRecord(rec)
	return
}

func (bm *BufferMgr) addKeyCell(cell KeyCell, blk BlockId) {
	exists, n := bm.findBuffer(blk)
	if !exists {
		panic(errors.New("invalid blockId access occured"))
	}
	bm.bufferPool[n].pg.addKeyCell(cell)
}

func (bm *BufferMgr) getPage(blk BlockId) Page {
	exists, n := bm.findBuffer(blk)
	if !exists {
		panic(errors.New("invalid blockId access occured"))
	}
	return bm.bufferPool[n].pg
}

func (bm *BufferMgr) flushAll() {
	for _, buff := range bm.bufferPool {
		buff.flush()
	}
}

type Buffer struct {
	isDirty bool
	fm      FileMgr
	blk     BlockId
	pg      Page
}

func newBuffer(fm FileMgr, blk BlockId, pg Page) Buffer {
	return Buffer{isDirty: false, fm: fm, blk: blk, pg: pg}
}

func (buff *Buffer) flush() {
	buff.fm.write(buff.blk, buff.pg) //todo:参照渡さなくてもよいのでは？
}
