package transaction

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/tychyDB/storage"
)

var UniqueLsn uint32
var UniqueLogPageNumber uint32

const MaxLogPoolSize = 100

const (
	BEGIN  = 0
	UPDATE = 1
	ABORT  = 2
	COMMIT = 3
)

func init() {
	UniqueLsn = 0
	UniqueLogPageNumber = 0
}

type LogMgr struct {
	fm         storage.FileMgr
	flashedLSN uint32
	LogPage    *LogPage // use UpperCase for test
}

func NewLogMgr(fm storage.FileMgr) *LogMgr {
	logMgr := LogMgr{}
	logMgr.fm = fm
	logMgr.flashedLSN = 0
	logMgr.LogPage = newLogPage()
	return &logMgr
}

func getUniqueLSN() uint32 {
	res := UniqueLsn
	UniqueLsn++
	return res
}

func (lm *LogMgr) addLog(txnId, logType uint32) {
	log := newUniqueLog(txnId, logType)
	lm.LogPage.addLog(log)
}

func (lm *LogMgr) addLogForUpdate(txnId, logType uint32, updateInfo storage.UpdateInfo) {
	if logType != UPDATE {
		panic(errors.New("log type expected to be UPDATE"))
	}
	log := newUniqueLog(txnId, UPDATE)
	log.updateInfo = updateInfo
	lm.LogPage.addLog(log)
}

func (lm *LogMgr) Print() {
	lm.LogPage.Print()
}

type Log struct {
	txnId      uint32
	lsn        uint32
	logType    uint32
	updateInfo storage.UpdateInfo
}

func newUniqueLog(txnId uint32, logType uint32) *Log {
	log := &Log{}
	log.txnId = txnId
	log.lsn = getUniqueLSN()
	log.logType = logType
	return log
}

func (log *Log) toBytes() []byte {
	buf := make([]byte, 4*IntSize)
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], log.txnId)
	binary.BigEndian.PutUint32(buf[2*IntSize:3*IntSize], log.lsn)
	binary.BigEndian.PutUint32(buf[3*IntSize:4*IntSize], log.logType)
	if log.logType == UPDATE {
		uinfoBuf := log.updateInfo.ToBytes()
		buf = append(buf, uinfoBuf...)
	}
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(len(buf)))
	return buf
}

func (log *Log) info() {
	fmt.Printf("%7d, %7d, %7d, ", log.txnId, log.lsn, log.logType)
	if log.logType == UPDATE {
		u := log.updateInfo
		fmt.Printf("%7d, %7d, %7d, %7b, %7b", u.PageIdx, u.PtrIdx, u.ColNum, u.From, u.To)
	} else {
		fmt.Printf("       ,        ,        ,         ,        ")
	}
	fmt.Print("\n")
}
