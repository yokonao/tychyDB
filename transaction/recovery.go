package transaction

import (
	"github.com/tychyDB/storage"
)

type RecoveryMgr struct {
	lm  *LogMgr
	ptb *storage.PageTable
}

func NewRecoveryMgr(lm *LogMgr, ptb *storage.PageTable) *RecoveryMgr {
	recoveryManager := &RecoveryMgr{}
	recoveryManager.lm = lm
	recoveryManager.ptb = ptb
	return recoveryManager
}

func (recoveryManager *RecoveryMgr) Begin(txn *Transaction) {
	recoveryManager.lm.addLog(txn.txnId, BEGIN)
}

func (recoveryManager *RecoveryMgr) Commit(txn *Transaction) {
	recoveryManager.lm.addLog(txn.txnId, COMMIT)
	recoveryManager.lm.WritePage()
}
func (recoveryManager *RecoveryMgr) Abort(txn *Transaction) {
	recoveryManager.lm.addLog(txn.txnId, ABORT)
}

func (recoveryManager *RecoveryMgr) Update(txn *Transaction, updateInfo storage.UpdateInfo) {
	log := recoveryManager.lm.addLog(txn.txnId, UPDATE)
	log.addUpdateInfo(updateInfo)
	recoveryManager.ptb.SetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile), log.lsn)
}

func (recoveryManager *RecoveryMgr) LogRedo(st *storage.Storage) {
	redoPool := map[TxnId]bool{}
	txnTable := map[TxnId]TxnStatus{}
	// log file full scan
	logIter := NewLogIter(recoveryManager.lm, 0)
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
	logIter = NewLogIter(recoveryManager.lm, 0)
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
