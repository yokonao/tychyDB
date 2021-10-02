package storage

import "encoding/binary"

type MetaPage struct {
	rootBlk BlockId
}

func newMetaPage() *MetaPage {
	return &MetaPage{}
}

func (pg *MetaPage) toBytes() []byte {
	buf := make([]byte, PageSize)
	binary.BigEndian.PutUint32(buf[:IntSize], pg.rootBlk.blockNum)
	return buf
}
