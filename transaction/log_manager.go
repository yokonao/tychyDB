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

func (lm *LogMgr) logAt(idx uint32) (Log, error) {
	return lm.LogPage.logAt(idx)
}

func (lm *LogMgr) isEnd(idx uint32) bool {
	return lm.LogPage.isEnd(idx)
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
	lm.FlashedLSN = lm.LogPage.maxLSN()
}

func (lm *LogMgr) Print() {
	lm.LogPage.Print()
}
