package transaction

import (
	"github.com/tychyDB/storage"
)

type RecoveryMgr struct {
	txn   *Transaction
	txnId uint32
	lm    *LogMgr
	ptb   *storage.PageTable
}

func NewRecoveryMgr(txn *Transaction, txnId uint32, lm *LogMgr, ptb *storage.PageTable) *RecoveryMgr {
	rm := &RecoveryMgr{}
	rm.txn = txn
	rm.txnId = txnId
	rm.lm = lm
	rm.ptb = ptb
	return rm
}

func (rm *RecoveryMgr) Begin() {
	rm.lm.addLog(rm.txnId, BEGIN)
}

func (rm *RecoveryMgr) Commit() {
	rm.lm.addLog(rm.txnId, COMMIT)
	rm.lm.WritePage()
}
func (rm *RecoveryMgr) Abort() {
	rm.lm.addLog(rm.txnId, ABORT)
}

func (rm *RecoveryMgr) Update(updateInfo storage.UpdateInfo) {
	log := rm.lm.addLog(rm.txnId, UPDATE)
	log.addUpdateInfo(updateInfo)
	rm.ptb.SetPageLSN(storage.NewBlockId(updateInfo.PageIdx), log.lsn)
}
