package storage

var UniqueBlockId uint32

func init() {
	UniqueBlockId = 0
}

type BlockId struct {
	fileName string
	BlockNum uint32
}

func NewBlockId(blockNum uint32, fileName string) BlockId {
	blk := BlockId{}
	blk.BlockNum = blockNum
	blk.fileName = fileName
	return blk
}

func newUniqueBlockId(fileName string) BlockId {
	blk := BlockId{}
	blk.BlockNum = UniqueBlockId
	blk.fileName = fileName
	UniqueBlockId++
	return blk

}
