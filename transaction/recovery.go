package transaction

import "github.com/tychyDB/storage"

type RecoveryMgr struct {
	lm    *LogMgr
	txn   *Transaction
	txnId uint32
}

func NewRecoveryMgr(txn *Transaction, txnId uint32, lm *LogMgr) *RecoveryMgr {
	rm := &RecoveryMgr{}
	rm.lm = lm
	rm.txn = txn
	rm.txnId = txnId
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
}
