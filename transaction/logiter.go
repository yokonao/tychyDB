package transaction

type LogIter struct {
	lm     *LogMgr
	curLSN uint32
}

func NewLogIter(lm *LogMgr, startLSN uint32) *LogIter {
	logIter := &LogIter{lm: lm, curLSN: startLSN}
	return logIter
}

func (logIter *LogIter) IsEnd() bool {
	return logIter.lm.isEnd(logIter.curLSN)
}

func (logIter *LogIter) Next() (Log, error) {
	idx := logIter.curLSN
	logIter.curLSN += 1
	return logIter.lm.logAt(idx)
}
