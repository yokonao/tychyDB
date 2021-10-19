package transaction

import (
	"encoding/binary"
	"fmt"

	"github.com/tychyDB/storage"
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
	blockNum := binary.BigEndian.Uint32(bytes[:IntSize])
	blk := storage.NewBlockId(blockNum)
	pg := &LogPage{}
	pg.blk = blk
	if bytes[IntSize] == 0 {
		pg.isFull = false
	} else {
		pg.isFull = true
	}
	pg.numLogs = binary.BigEndian.Uint32(bytes[1+IntSize : 1+2*IntSize])
	pg.logs = make([]*Log, pg.numLogs)
	cur := 1 + 2*IntSize

	for i := 0; i < int(pg.numLogs); i++ {
		lenLog := binary.BigEndian.Uint32(bytes[cur : cur+IntSize])
		log := &Log{}
		log.txnId = binary.BigEndian.Uint32(bytes[cur+IntSize : cur+2*IntSize])
		log.lsn = binary.BigEndian.Uint32(bytes[cur+2*IntSize : cur+3*IntSize])
		log.logType = binary.BigEndian.Uint32(bytes[cur+3*IntSize : cur+4*IntSize])
		if log.logType == UPDATE {
			// この部分はUpdateInfoにfromBytesを実装して移譲できる
			uinfohead := cur + 4*IntSize
			pageIdx := binary.BigEndian.Uint32(bytes[uinfohead+IntSize : uinfohead+2*IntSize])
			ptrIdx := binary.BigEndian.Uint32(bytes[uinfohead+2*IntSize : uinfohead+3*IntSize])
			colNum := binary.BigEndian.Uint32(bytes[uinfohead+3*IntSize : uinfohead+4*IntSize])
			fromLen := binary.BigEndian.Uint32(bytes[uinfohead+4*IntSize : uinfohead+5*IntSize])
			toLen := binary.BigEndian.Uint32(bytes[uinfohead+5*IntSize : uinfohead+6*IntSize])

			uinfohead += 6 * IntSize
			from := make([]byte, fromLen)
			to := make([]byte, toLen)
			copy(from, bytes[uinfohead:uinfohead+int(fromLen)])
			uinfohead += int(fromLen)
			copy(to, bytes[uinfohead:uinfohead+int(toLen)])

			uinfo := storage.NewUpdateInfo(pageIdx, ptrIdx, colNum, from, to)
			log.updateInfo = uinfo
		}

		pg.logs[i] = log
		cur += int(lenLog)
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
