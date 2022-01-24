package transaction

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
	txnId uint32
}

func NewTransaction() *Transaction {
	txn := &Transaction{}
	txn.txnId = getUniqueTxnId()
	return txn
}
