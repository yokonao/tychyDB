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
	txnId uint32
}

func NewTransaction(lm *LogMgr) *Transaction {
	txn := &Transaction{}
	txn.txnId = getUniqueTxnId()
	txn.lm = lm
	return txn
}

func (txn *Transaction) Begin() {
	txn.lm.addLog(txn.txnId, BEGIN)
}

func (txn *Transaction) Commit() {
	txn.lm.addLog(txn.txnId, COMMIT)
}
func (txn *Transaction) Abort() {
	txn.lm.addLog(txn.txnId, ABORT)
}
func (txn *Transaction) Update(updateInfo storage.UpdateInfo) {
	txn.lm.addLogForUpdate(txn.txnId, UPDATE, updateInfo)
}

//func (txn *Transaction) insert() {
//}
