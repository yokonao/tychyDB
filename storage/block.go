package storage

var UniqueBlockId uint32

func init() {
	UniqueBlockId = 0
}

type BlockId struct {
	BlockNum uint32
}

func newBlockId(blockNum uint32) BlockId {
	blk := BlockId{}
	blk.BlockNum = blockNum
	return blk
}

func newUniqueBlockId() BlockId {
	blk := BlockId{}
	blk.BlockNum = UniqueBlockId
	UniqueBlockId++
	return blk

}
