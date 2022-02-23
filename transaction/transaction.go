package transaction

type TxnId uint32

var UniqueTxnId TxnId

type TxnStatus uint32

const (
	TXN_INPROGRESS TxnStatus = iota
	TXN_ABORTED
	TXN_COMMITED
)

func init() {
	UniqueTxnId = 0
}

func getUniqueTxnId() TxnId {
	res := UniqueTxnId
	UniqueTxnId++
	return res
}

type Transaction struct {
	txnId TxnId
}

func NewTransaction() *Transaction {
	txn := &Transaction{}
	txn.txnId = getUniqueTxnId()
	return txn
}
