package storage

import (
	"encoding/binary"

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
	rootBlockId := binary.BigEndian.Uint32(bytes[:IntSize])
	UniqueBlockId = binary.BigEndian.Uint32(bytes[IntSize : 2*IntSize])
	pg.rootBlk = NewBlockId(rootBlockId)

	lencols := binary.BigEndian.Uint32(bytes[2*IntSize : 3*IntSize])
	cur := 3 * IntSize
	for i := 0; i < int(lencols); i++ {
		datalen := binary.BigEndian.Uint32(bytes[cur : cur+IntSize])
		c := newColumnfromBytes(bytes[cur : cur+int(datalen)])
		pg.cols = append(pg.cols, c)
		cur += int(datalen)
	}
	return pg
}

func (pg *MetaPage) toBytes() []byte {
	gen := util.NewGenStruct(0, PageSize)
	gen.PutUInt32(pg.rootBlk.BlockNum)
	gen.PutUInt32(UniqueBlockId)
	gen.PutUInt32(uint32(len(pg.cols)))

	for _, col := range pg.cols {
		b := col.toBytes()
		datalen := binary.BigEndian.Uint32(b[:IntSize])
		gen.PutBytes(datalen, b)
	}
	return gen.DumpBytes()
}
