package storage

import "errors"

var UniqueBlockId uint32

func init() {
	UniqueBlockId = 0
}

type BlockId struct {
	blockNum uint32
}

func newBlockId(blockNum uint32) BlockId {
	if blockNum >= UniqueBlockId {
		panic(errors.New("input blockNum is too large"))
	}
	blk := BlockId{}
	blk.blockNum = blockNum
	return blk
}

func newUniqueBlockId() BlockId {
	blk := BlockId{}
	blk.blockNum = UniqueBlockId
	UniqueBlockId++
	return blk

}
