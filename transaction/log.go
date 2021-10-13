package transaction

import (
	"errors"
	"fmt"

	"github.com/tychyDB/storage"
)

var UniqueLsn uint32

const MaxLogPoolSize = 100

const (
	BEGIN  = 0
	UPDATE = 1
	ABORT  = 2
	COMMIT = 3
)

func init() {
	UniqueLsn = 0
}

type LogMgr struct {
	fm         storage.FileMgr
	flashedLSN uint32
	logPool    []*Log
	logCount   uint32
}

func NewLogMgr(fm storage.FileMgr) *LogMgr {
	logMgr := LogMgr{}
	logMgr.fm = fm
	logMgr.flashedLSN = 0
	logMgr.logPool = make([]*Log, MaxLogPoolSize)
	logMgr.logCount = 0
	return &logMgr
}

func getUniqueLSN() uint32 {
	res := UniqueLsn
	UniqueLsn++
	return res
}

func (lm *LogMgr) addLog(txnId, logType uint32) {
	log := newUniqueLog(txnId, logType)
	lm.logPool[lm.logCount] = log
	lm.logCount++
}

func (lm *LogMgr) addLogForUpdate(txnId, logType uint32, updateInfo UpdateInfo) {
	if logType != UPDATE {
		panic(errors.New("log type expected to be UPDATE"))
	}
	log := newUniqueLog(txnId, UPDATE)
	log.updateInfo = updateInfo
	lm.logPool[lm.logCount] = log
	lm.logCount++
}

func (lm *LogMgr) Print() {
	fmt.Println("txnId  ,     lsn, logType, pageIdx,  ptrIdx,   colNum,    from,      to")
	for i := 0; i < int(lm.logCount); i++ {
		lm.logPool[i].info()
	}
}

type UpdateInfo struct {
	pageIdx uint32
	ptrIdx  uint32
	colNum  uint32
	from    []byte
	to      []byte
}

func NewUpdateInfo(pageIdx uint32, ptrIdx uint32, colNum uint32, from []byte, to []byte) UpdateInfo {
	info := UpdateInfo{}
	info.pageIdx = pageIdx
	info.ptrIdx = ptrIdx
	info.colNum = colNum
	info.from = from
	info.to = to
	return info
}

type Log struct {
	txnId      uint32
	lsn        uint32
	logType    uint32
	updateInfo UpdateInfo
}

func newUniqueLog(txnId uint32, logType uint32) *Log {
	log := &Log{}
	log.txnId = txnId
	log.lsn = getUniqueLSN()
	log.logType = logType
	return log
}

func (log *Log) info() {
	fmt.Printf("%7d, %7d, %7d, ", log.txnId, log.lsn, log.logType)
	if log.logType == UPDATE {
		u := log.updateInfo
		fmt.Printf("%7d, %7d, %7d, %7b, %7b", u.pageIdx, u.ptrIdx, u.colNum, u.from, u.to)
	} else {
		fmt.Printf("       ,        ,        ,         ,        ")
	}
	fmt.Print("\n")
}
