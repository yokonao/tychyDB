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
	numOfPin int
	table    map[int]int
	queue    algorithm.Queue
}

func newPageTable() *PageTable {
	ptb := &PageTable{}
	ptb.numOfPin = 0
	ptb.table = make(map[int]int)
	ptb.queue = algorithm.NewQueue(64)
	return ptb
}

func (ptb *PageTable) flush() {
	for {
		if ptb.queue.IsEmpty() {
			break
		}
		curBlkNum := ptb.queue.Pop()
		curBlk := NewBlockId(uint32(curBlkNum))
		curBuffId := ptb.table[int(curBlkNum)]
		delete(ptb.table, int(curBlkNum))
		fm.Write(curBlk, bm.pool[curBuffId].page().toBytes())
		bm.pool[curBuffId] = nil

	}
	ptb.numOfPin = 0
}

func (ptb *PageTable) makeSpace() {
	if len(ptb.table) > MaxBufferPoolSize {
		panic(errors.New("unexpected"))
	} else if len(ptb.table) < MaxBufferPoolSize {
		return
	}
	if !ptb.available() {
		// unpinされるまで待つ実装でいつか置き換える
		panic(errors.New("no space in buffer pool"))
	}
	//fmt.Println("MakeSpace")
	for {
		//ptb.Print()
		dropBlkNum := ptb.queue.Pop()
		dropBlk := NewBlockId(uint32(dropBlkNum))
		dropBuffId := ptb.table[int(dropBlkNum)]
		dropBuff := bm.pool[dropBuffId]
		if dropBuff.pin {
			ptb.queue.Push(dropBlkNum)
		} else {
			if dropBuff.ref {
				dropBuff.ref = false
				ptb.queue.Push(dropBlkNum)
			} else {
				delete(ptb.table, int(dropBlkNum))
				fm.Write(dropBlk, bm.pool[dropBuffId].page().toBytes())
				bm.pool[dropBuffId] = nil
				break
			}
		}
	}
	//ptb.Print()

}

func (ptb *PageTable) getBuffId(blk BlockId) int {
	buffId, exists := ptb.table[int(blk.BlockNum)]
	if exists {
		return buffId
	} else {
		ptb.makeSpace()
		ptb.queue.Push(int(blk.BlockNum))
		buffId := bm.load(blk)
		ptb.table[int(blk.BlockNum)] = buffId
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
	ptb.queue.Push(int(blk.BlockNum))
	buff := newBufferFromPage(blk, pg)
	buffId := bm.allocate(buff)
	ptb.table[int(blk.BlockNum)] = buffId
}

func (ptb *PageTable) read(blk BlockId) *Page {
	return bm.pool[ptb.getBuffId(blk)].page()
}

func (ptb *PageTable) pin(blk BlockId) *Page {
	buff := bm.pool[ptb.getBuffId(blk)]
	buff.pin = true
	buff.ref = true
	ptb.numOfPin++
	return buff.page()
}

func (ptb *PageTable) unpin(blk BlockId) {
	buffId, exists := ptb.table[int(blk.BlockNum)]
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

func (ptb *PageTable) Print() {
	fmt.Printf("Print Page table {\n")
	fmt.Printf("table %v\n", ptb.table)
	fmt.Printf("queue [ ")
	ptb.queue.Print()
	fmt.Printf(" ]\n")
	fmt.Printf("NumOfPins {%d}\n", ptb.numOfPin)
	bm.Print()
	fmt.Printf("}\n")
}
