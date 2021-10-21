package storage

import "github.com/tychyDB/util"

// I feel UpdateInfo should be placed to util or log. Future work.
// However, updateInfo  can only be created inside table...
type UpdateInfo struct {
	PageIdx uint32
	PtrIdx  uint32
	ColNum  uint32
	From    []byte
	To      []byte
}

func NewUpdateInfo(pageIdx uint32, ptrIdx uint32, colNum uint32, from []byte, to []byte) UpdateInfo {
	info := UpdateInfo{}
	info.PageIdx = pageIdx
	info.PtrIdx = ptrIdx
	info.ColNum = colNum
	info.From = from
	info.To = to
	return info
}

func (uinfo *UpdateInfo) ToBytes() []byte {
	fromLen := uint32(len(uinfo.From))
	toLen := uint32(len(uinfo.To))
	bufLen := 5*IntSize + fromLen + toLen
	gen := util.NewGenStruct(0, uint32(bufLen))
	gen.PutUInt32(uinfo.PageIdx)
	gen.PutUInt32(uinfo.PtrIdx)
	gen.PutUInt32(uinfo.ColNum)
	gen.PutUInt32(fromLen)
	gen.PutBytes(fromLen, uinfo.From)
	gen.PutUInt32(toLen)
	gen.PutBytes(toLen, uinfo.To)
	return gen.DumpBytes()
}
