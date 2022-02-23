package transaction

import (
	"github.com/tychyDB/storage"
)

type RecoveryMgr struct {
	logManager *LogMgr
	pageTable  *storage.PageTable
}

func NewRecoveryMgr(logManager *LogMgr, pageTable *storage.PageTable) *RecoveryMgr {
	recoveryManager := &RecoveryMgr{}
	recoveryManager.logManager = logManager
	recoveryManager.pageTable = pageTable
	return recoveryManager
}

func (recoveryManager *RecoveryMgr) Begin(txn *Transaction) {
	recoveryManager.logManager.addLog(txn.txnId, BEGIN)
}

func (recoveryManager *RecoveryMgr) Commit(txn *Transaction) {
	recoveryManager.logManager.addLog(txn.txnId, COMMIT)
	recoveryManager.logManager.WritePage()
}
func (recoveryManager *RecoveryMgr) Abort(txn *Transaction) {
	recoveryManager.logManager.addLog(txn.txnId, ABORT)
}

func (recoveryManager *RecoveryMgr) Update(txn *Transaction, updateInfo storage.UpdateInfo) {
	log := recoveryManager.logManager.addLog(txn.txnId, UPDATE)
	log.addUpdateInfo(updateInfo)
	recoveryManager.pageTable.SetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile), log.lsn)
}

func (recoveryManager *RecoveryMgr) LogRedo(st *storage.Storage) {
	redoPool := map[TxnId]bool{}
	txnTable := map[TxnId]TxnStatus{}
	// log file full scan
	logIter := NewLogIter(recoveryManager.logManager, 0)
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
	logIter = NewLogIter(recoveryManager.logManager, 0)
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
