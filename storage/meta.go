package storage

import (
	"github.com/tychyDB/util"
)

type MetaPage struct {
	metaBlk BlockId
	rootBlk BlockId
	cols    []Column
}

func newMetaPage(blk BlockId) *MetaPage {
	return &MetaPage{metaBlk: blk}
}

func newMetaPageFromBytes(bytes []byte) *MetaPage {
	pg := &MetaPage{}
	iter := util.NewIterStruct(0, bytes)
	rootBlockId := iter.NextUInt32()
	UniqueBlockId = iter.NextUInt32()
	pg.rootBlk = NewBlockId(rootBlockId)

	lenCols := iter.NextUInt32()
	for i := 0; i < int(lenCols); i++ {
		dataLen := iter.NextUInt32()
		c := newColumnfromBytes(iter.NextBytes(dataLen))
		pg.cols = append(pg.cols, c)
	}
	return pg
}

func (pg *MetaPage) toBytes() []byte {
	gen := util.NewGenStruct(0, PageSize)
	gen.PutUInt32(pg.rootBlk.BlockNum)
	gen.PutUInt32(UniqueBlockId)
	gen.PutUInt32(uint32(len(pg.cols)))

	for _, col := range pg.cols {
		buf := col.toBytes()
		bufLen := uint32(len(buf))
		gen.PutUInt32(bufLen)
		gen.PutBytes(bufLen, buf)
	}
	return gen.DumpBytes()
}
