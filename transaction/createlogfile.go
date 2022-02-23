package transaction

import "github.com/tychyDB/storage"

func CreateLogFile() {
	UniqueTxnId = 0
	storage.CreateStorage()

	logfm := storage.NewFileMgr()
	fm := storage.NewFileMgr()

	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := NewLogMgr(*logfm)
	rm := NewRecoveryMgr(lm, ptb)
	txn := NewTransaction()

	rm.Begin(txn)
	updateInfo := st.Update(500, "fuga", 33)
	rm.Update(txn, updateInfo)

	updateInfo = st.Update(2, "fuga", 3337)
	rm.Update(txn, updateInfo)

	rm.Commit(txn)
	st.Clear()
	st.Flush()
}
