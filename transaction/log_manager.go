package transaction

import (
	"errors"

	"github.com/tychyDB/storage"
)

const LogFile = "log"

type LogMgr struct {
	UniqueLSN     uint32
	UniquePageNum uint32
	LogPage       *LogPage // I used UpperCase for testing, but this should be lowerCamelCase.
	fm            storage.FileMgr
	FlashedLSN    uint32
}

func NewLogMgr(fm storage.FileMgr) *LogMgr {
	logMgr := LogMgr{}
	logMgr.UniqueLSN = 1 // 1-indexed, because flushed lsn is 0
	logMgr.UniquePageNum = 0
	logMgr.fm = fm
	logMgr.FlashedLSN = 0
	logMgr.LogPage = newLogPage(logMgr.getUniquePageNum())
	return &logMgr
}

func NewLogMgrFromFile(fm storage.FileMgr) *LogMgr {
	logMgr := LogMgr{}
	logMgr.fm = fm
	blk, n, buf := fm.ReadLastBlock(LogFile)
	if n == 0 {
		panic(errors.New("page is empty"))
	}
	logMgr.UniquePageNum = uint32(blk) + 1
	logMgr.LogPage = NewLogPageFromBytes(buf)
	logMgr.FlashedLSN = logMgr.LogPage.maxLSN()
	logMgr.UniqueLSN = logMgr.FlashedLSN + 1
	return &logMgr
}

func (logManager *LogMgr) logAt(idx uint32) (Log, error) {
	return logManager.LogPage.logAt(idx)
}

func (logManager *LogMgr) isEnd(idx uint32) bool {
	return logManager.LogPage.isEnd(idx)
}

func (logManager *LogMgr) getUniqueLSN() uint32 {
	res := logManager.UniqueLSN
	logManager.UniqueLSN++
	return res
}

func (logManager *LogMgr) getUniquePageNum() uint32 {
	res := logManager.UniquePageNum
	logManager.UniquePageNum++
	return res
}

func (logManager *LogMgr) addLog(txnId TxnId, logType uint32) *Log {
	log := newUniqueLog(logManager.getUniqueLSN(), txnId, logType)
	logManager.LogPage.addLog(log)
	return log
}

func (logManager *LogMgr) WritePage() {
	logManager.fm.Write(logManager.LogPage.blk, logManager.LogPage.ToBytes())
	logManager.FlashedLSN = logManager.LogPage.maxLSN()
}

func (logManager *LogMgr) Print() {
	logManager.LogPage.Print()
}
