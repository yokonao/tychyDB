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
	rm.ptb.SetPageLSN(storage.NewBlockId(updateInfo.PageIdx), log.lsn)
}
