package storage

import (
	"encoding/binary"
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
	pg.rootBlk = newBlockId(rootBlockId)

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
	buf := make([]byte, PageSize)
	binary.BigEndian.PutUint32(buf[:IntSize], pg.rootBlk.blockNum)
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], UniqueBlockId)

	binary.BigEndian.PutUint32(buf[2*IntSize:3*IntSize], uint32(len(pg.cols)))
	cur := 3 * IntSize
	for _, col := range pg.cols {
		b := col.toBytes()
		datalen := binary.BigEndian.Uint32(b[:IntSize])
		copy(buf[cur:cur+int(datalen)], b)
		cur += int(datalen)
	}
	return buf
}
