package transaction

type LogIter struct {
	logManager *LogMgr
	curLSN     uint32
}

func NewLogIter(logManager *LogMgr, startLSN uint32) *LogIter {
	logIter := &LogIter{logManager: logManager, curLSN: startLSN}
	return logIter
}

func (logIter *LogIter) IsEnd() bool {
	return logIter.logManager.isEnd(logIter.curLSN)
}

func (logIter *LogIter) Next() (Log, error) {
	idx := logIter.curLSN
	logIter.curLSN += 1
	return logIter.logManager.logAt(idx)
}
