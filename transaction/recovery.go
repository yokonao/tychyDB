package transaction

import (
	"github.com/tychyDB/storage"
)

type RecoveryMgr struct {
	lm  *LogMgr
	ptb *storage.PageTable
}

func NewRecoveryMgr(lm *LogMgr, ptb *storage.PageTable) *RecoveryMgr {
	rm := &RecoveryMgr{}
	rm.lm = lm
	rm.ptb = ptb
	return rm
}

func (rm *RecoveryMgr) Begin(txn *Transaction) {
	rm.lm.addLog(txn.txnId, BEGIN)
}

func (rm *RecoveryMgr) Commit(txn *Transaction) {
	rm.lm.addLog(txn.txnId, COMMIT)
	rm.lm.WritePage()
}
func (rm *RecoveryMgr) Abort(txn *Transaction) {
	rm.lm.addLog(txn.txnId, ABORT)
}

func (rm *RecoveryMgr) Update(txn *Transaction, updateInfo storage.UpdateInfo) {
	log := rm.lm.addLog(txn.txnId, UPDATE)
	log.addUpdateInfo(updateInfo)
	rm.ptb.SetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile), log.lsn)
}

func (rm *RecoveryMgr) LogRedo(st *storage.Storage) {
	redoPool := map[TxnId]bool{}
	txnTable := map[TxnId]TxnStatus{}
	// log file full scan
	logIter := NewLogIter(rm.lm, 0)
	for !logIter.IsEnd() {
		log, err := logIter.Next()
		if err != nil {
			panic(ErrOutOfBounds)
		}
		switch log.logType {
		case BEGIN:
			// todo check if exists
			txnTable[log.txnId] = TXN_INPROGRESS
		case COMMIT:
			txnTable[log.txnId] = TXN_COMMITED
			redoPool[log.txnId] = true
		case ABORT:
			txnTable[log.txnId] = TXN_ABORTED
		case UPDATE:
			continue
		}

	}

	// redo if txn is valid
	logIter = NewLogIter(rm.lm, 0)
	for !logIter.IsEnd() {
		log, err := logIter.Next()
		if err != nil {
			panic(ErrOutOfBounds)
		}

		switch log.logType {
		case BEGIN, COMMIT, ABORT:
			continue
		case UPDATE:
			if redoPool[log.txnId] {
				st.UpdateFromInfo(&log.updateInfo)
			}
		default:
			panic(ErrNotImplemented)
		}
	}
	st.Flush()
}
