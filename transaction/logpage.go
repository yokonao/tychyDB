package transaction

import (
	"encoding/binary"
	"fmt"

	"github.com/tychyDB/storage"
	"github.com/tychyDB/util"
)

const IntSize = 4 // storageと共通化したい

// Layout
// 4bytes blockId
type LogPage struct {
	blk     storage.BlockId
	isFull  bool
	numLogs uint32
	logs    []*Log
}

func newLogPage(pageNum uint32) *LogPage {
	logPage := &LogPage{}
	logPage.blk = storage.NewBlockId(pageNum)
	logPage.isFull = false
	logPage.numLogs = 0
	return logPage
}

func NewLogPageFromBytes(bytes []byte) *LogPage {
	iter := util.NewIterStruct(0, bytes)
	blockNum := iter.NextUInt32()
	blk := storage.NewBlockId(blockNum)
	pg := &LogPage{}
	pg.blk = blk
	pg.isFull = iter.NextBool()
	pg.numLogs = iter.NextUInt32()
	pg.logs = make([]*Log, pg.numLogs)
	for i := 0; i < int(pg.numLogs); i++ {
		iter.NextUInt32() // skip len(Log)
		log := &Log{}
		log.txnId = iter.NextUInt32()
		log.lsn = iter.NextUInt32()
		log.logType = iter.NextUInt32()
		if log.logType == UPDATE {
			// この部分はUpdateInfoにfromBytesを実装して移譲できる
			iter.NextUInt32() // skip len(Update Info)
			pageIdx := iter.NextUInt32()
			ptrIdx := iter.NextUInt32()
			colNum := iter.NextUInt32()
			fromLen := iter.NextUInt32()
			toLen := iter.NextUInt32()
			fmt.Println("from ", fromLen, "to ", toLen)
			from := iter.NextBytes(fromLen)
			to := iter.NextBytes(toLen)
			uinfo := storage.NewUpdateInfo(pageIdx, ptrIdx, colNum, from, to)
			log.updateInfo = uinfo
		}
		pg.logs[i] = log
	}
	return pg
}

func (pg *LogPage) ToBytes() []byte {
	buf := make([]byte, storage.PageSize)
	binary.BigEndian.PutUint32(buf[:IntSize], pg.blk.BlockNum)
	if pg.isFull {
		buf[IntSize] = 1
	} else {
		buf[IntSize] = 0
	}
	binary.BigEndian.PutUint32(buf[1+IntSize:1+2*IntSize], pg.numLogs)
	cur := 1 + 2*IntSize
	for i := 0; i < int(pg.numLogs); i++ {
		logBuf := pg.logs[i].toBytes()
		copy(buf[cur:cur+len(logBuf)], logBuf)
		cur += len(logBuf)
	}
	return buf
}

func (pg *LogPage) addLog(log *Log) {
	// should check page availabirity
	pg.logs = append(pg.logs, log)
	pg.numLogs++
}

func (pg *LogPage) Print() {
	fmt.Println("txnId  ,     lsn, logType, pageIdx,  ptrIdx,   colNum,    from,      to")
	for i := 0; i < int(pg.numLogs); i++ {
		pg.logs[i].info()
	}
}
