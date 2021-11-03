package transaction

import (
	"github.com/tychyDB/storage"
)

type LogMgr struct {
	UniqueLSN     uint32
	UniquePageNum uint32
	LogPage       *LogPage // use UpperCase for test
	fm            storage.FileMgr
	flashedLSN    uint32
}

func NewLogMgr(fm storage.FileMgr) *LogMgr {
	logMgr := LogMgr{}
	logMgr.UniqueLSN = 1
	logMgr.UniquePageNum = 0
	logMgr.fm = fm
	logMgr.flashedLSN = 0
	logMgr.LogPage = newLogPage(logMgr.getUniquePageNum())
	return &logMgr
}

func (lm *LogMgr) getUniqueLSN() uint32 {
	res := lm.UniqueLSN
	lm.UniqueLSN++
	return res
}

func (lm *LogMgr) getUniquePageNum() uint32 {
	res := lm.UniquePageNum
	lm.UniquePageNum++
	return res
}

func (lm *LogMgr) addLog(txnId, logType uint32) *Log {
	log := newUniqueLog(lm.getUniqueLSN(), txnId, logType)
	lm.LogPage.addLog(log)
	return log
}

func (lm *LogMgr) WritePage() {
	lm.fm.Write(lm.LogPage.blk, lm.LogPage.ToBytes())
}

func (lm *LogMgr) Print() {
	lm.LogPage.Print()
}
