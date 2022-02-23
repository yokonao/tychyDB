package storage

import (
	"errors"
	"fmt"

	"github.com/tychyDB/algorithm"
)

// The page table keeps track of pages
// that are currently in memory.
// Also maintains additional meta-data per page
type PageTable struct {
	bufferManager *BufferMgr
	numOfPin      int
	table         map[int]int
	queue         algorithm.Queue
}

func NewPageTable(bufferManager *BufferMgr) *PageTable {
	pageTable := &PageTable{}
	pageTable.bufferManager = bufferManager
	pageTable.numOfPin = 0
	pageTable.table = make(map[int]int)
	pageTable.queue = algorithm.NewQueue(64)
	return pageTable
}

func (pageTable *PageTable) ClearBuffer() {
	// for testing
	for {
		if pageTable.queue.IsEmpty() {
			break
		}
		curBlkNum := pageTable.queue.Pop()
		curBuffId := pageTable.table[int(curBlkNum)]
		delete(pageTable.table, int(curBlkNum))
		pageTable.bufferManager.clear(curBuffId)
	}
	pageTable.numOfPin = 0
}

func (pageTable *PageTable) Flush() {
	for {
		if pageTable.queue.IsEmpty() {
			break
		}
		curBlkNum := pageTable.queue.Pop()
		curBuffId := pageTable.table[int(curBlkNum)]
		delete(pageTable.table, int(curBlkNum))
		pageTable.bufferManager.flush(curBuffId)
	}
	pageTable.numOfPin = 0
}

func (pageTable *PageTable) makeSpace() {
	if len(pageTable.table) > MaxBufferPoolSize {
		panic(errors.New("unexpected"))
	} else if len(pageTable.table) < MaxBufferPoolSize {
		return
	}
	if !pageTable.available() {
		// TODO unpinされるまで待つ実装でいつか置き換える
		panic(errors.New("no space in buffer pool"))
	}
	for {
		dropBlkNum := pageTable.queue.Pop()
		dropBuffId := pageTable.table[int(dropBlkNum)]
		if pageTable.bufferManager.isPinned(dropBuffId) {
			pageTable.queue.Push(dropBlkNum)
		} else {
			if pageTable.bufferManager.isRefed(dropBuffId) {
				pageTable.bufferManager.unRef(dropBuffId)
				pageTable.queue.Push(dropBlkNum)
			} else {
				delete(pageTable.table, int(dropBlkNum))
				pageTable.bufferManager.flush(dropBuffId)
				break
			}
		}
	}
}

func (pageTable *PageTable) getBuffId(blk BlockId) int {
	buffId, exists := pageTable.table[int(blk.BlockNum)]
	if exists {
		return buffId
	} else {
		pageTable.makeSpace()
		pageTable.queue.Push(int(blk.BlockNum))
		buffId := pageTable.bufferManager.load(blk)
		pageTable.table[int(blk.BlockNum)] = buffId
		return buffId
	}
}

func (pageTable *PageTable) available() bool {
	return pageTable.numOfPin != MaxBufferPoolSize
}

func (pageTable *PageTable) set(blk BlockId, pg *Page) {
	pageTable.makeSpace()
	pageTable.queue.Push(int(blk.BlockNum))
	buff := newBufferFromPage(blk, pg)
	buffId := pageTable.bufferManager.allocate(buff)
	pageTable.table[int(blk.BlockNum)] = buffId
}

func (pageTable *PageTable) read(blk BlockId) *Page {
	return pageTable.bufferManager.pageAt(pageTable.getBuffId(blk))
}

func (pageTable *PageTable) pin(blk BlockId) *Page {
	buffId := pageTable.getBuffId(blk)
	pageTable.bufferManager.pin(buffId)
	pageTable.numOfPin++
	return pageTable.bufferManager.pageAt(buffId)
}

func (pageTable *PageTable) unpin(blk BlockId) {
	buffId, exists := pageTable.table[int(blk.BlockNum)]
	if !exists {
		panic(errors.New("trying to unpin page not on disk"))
	}
	pageTable.bufferManager.unpin(buffId)
	pageTable.numOfPin--
}

func (pageTable *PageTable) GetPageLSN(blk BlockId) uint32 {
	buffId := pageTable.getBuffId(blk)
	return pageTable.bufferManager.pageAt(buffId).header.pageLSN
}

func (pageTable *PageTable) SetPageLSN(blk BlockId, lsn uint32) {
	buffId := pageTable.getBuffId(blk)
	pageTable.bufferManager.pageAt(buffId).header.pageLSN = lsn
}

func (pageTable *PageTable) Print() {
	fmt.Printf("Print Page table {\n")
	fmt.Printf("table %v\n", pageTable.table)
	fmt.Printf("queue [ ")
	pageTable.queue.Print()
	fmt.Printf(" ]\n")
	fmt.Printf("NumOfPins {%d}\n", pageTable.numOfPin)
	pageTable.bufferManager.Print()
	fmt.Printf("}\n")
}
