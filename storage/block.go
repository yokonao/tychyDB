package storage

type BlockId struct {
	fileName string
	blockNum int64
}

func newBlockId(filename string, blockNum int64) *BlockId {
	blk := &BlockId{}
	blk.fileName = filename
	blk.blockNum = blockNum
	return blk
}
