package transaction

import (
	"errors"
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
			iter.NextUInt32() // skip uinfoBufLen
			pageIdx := iter.NextUInt32()
			ptrIdx := iter.NextUInt32()
			colNum := iter.NextUInt32()
			fromLen := iter.NextUInt32()
			from := iter.NextBytes(fromLen)
			toLen := iter.NextUInt32()
			to := iter.NextBytes(toLen)
			uinfo := storage.NewUpdateInfo(pageIdx, ptrIdx, colNum, from, to)
			log.updateInfo = uinfo
		}
		pg.logs[i] = log
	}
	return pg
}

func (pg *LogPage) ToBytes() []byte {
	gen := util.NewGenStruct(0, storage.PageSize)
	gen.PutUInt32(pg.blk.BlockNum)
	gen.PutBool(pg.isFull)
	gen.PutUInt32(pg.numLogs)

	for i := 0; i < int(pg.numLogs); i++ {
		logBuf := pg.logs[i].toBytes()
		logBufLen := uint32(len(logBuf))
		gen.PutUInt32(logBufLen)
		gen.PutBytes(logBufLen, logBuf)
	}
	return gen.DumpBytes()
}

func (pg *LogPage) addLog(log *Log) {
	// should check page availabirity
	pg.logs = append(pg.logs, log)
	pg.numLogs++
}

func (pg *LogPage) maxLSN() (res uint32) {
	if pg.numLogs == 0 {
		panic(errors.New("logPage is empty"))
	}

	res = 0
	for _, l := range pg.logs {
		if l.lsn > res {
			res = l.lsn
		}
	}
	return
}
func (pg *LogPage) Print() {
	fmt.Println("txnId  ,     lsn, logType, pageIdx,  ptrIdx,   colNum,    from,      to")
	for i := 0; i < int(pg.numLogs); i++ {
		pg.logs[i].info()
	}
}
