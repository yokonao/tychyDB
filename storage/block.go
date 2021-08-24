package storage

type BlockId struct {
	blockNum int64
}

func newBlockId(blockNum int64) *BlockId {
	blk := &BlockId{}
	blk.blockNum = blockNum
	return blk
}
