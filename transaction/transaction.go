package transaction

import "github.com/tychyDB/storage"

var UniqueTxnId uint32

func init() {
	UniqueTxnId = 0
}

func getUniqueTxnId() uint32 {
	res := UniqueTxnId
	UniqueTxnId++
	return res
}

type Transaction struct {
	lm    *LogMgr
	rm    *RecoveryMgr
	ptb   *storage.PageTable
	txnId uint32
}

func NewTransaction(lm *LogMgr, ptb *storage.PageTable) *Transaction {
	txn := &Transaction{}
	txn.txnId = getUniqueTxnId()
	txn.lm = lm
	txn.rm = NewRecoveryMgr(txn, txn.txnId, lm, ptb)
	txn.ptb = ptb
	return txn
}

func (txn *Transaction) Begin() {
	txn.rm.Begin()
}

func (txn *Transaction) Commit() {
	txn.rm.Commit()
}

func (txn *Transaction) Abort() {
	txn.rm.Abort()
}

func (txn *Transaction) Update(updateInfo storage.UpdateInfo) {
	txn.rm.Update(updateInfo)
}

//func (txn *Transaction) insert() {
//}
