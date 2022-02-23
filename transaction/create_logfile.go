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
	/*
	   | hoge  | fuga  | piyo  |
	   | --    | --    | --    |
	   | -345  | 77    | 43    |
	   | -100  | 89    | 111   |
	   | 0     | 0     | 0     |
	   | 2     | -13   | 89    |
	   | 10    | 45    | -999  |
	   | 500   | 5     | 90    |
	   | 10000 | 4     | 44    |
	   | 80000 | 10    | 0     |
	*/

	txnA := NewTransaction()
	rm.Begin(txnA)
	updateInfo := st.Update(500, "fuga", 33)
	rm.Update(txnA, updateInfo)
	updateInfo = st.Update(2, "fuga", 3337)
	rm.Update(txnA, updateInfo)
	rm.Commit(txnA)

	txnB := NewTransaction()
	txnC := NewTransaction()
	rm.Begin(txnB)
	updateInfo = st.Update(2, "fuga", 44)
	rm.Update(txnB, updateInfo)

	rm.Begin(txnC)
	updateInfo = st.Update(2, "fuga", 4447)
	rm.Update(txnB, updateInfo)
	updateInfo = st.Update(2, "fuga", 5557)
	rm.Update(txnC, updateInfo)
	rm.Abort(txnC)
	rm.Commit(txnB)

	txnD := NewTransaction()
	rm.Begin(txnD)
	updateInfo = st.Update(2, "fuga", 66)
	rm.Update(txnD, updateInfo)
	updateInfo = st.Update(2, "fuga", 6667)
	rm.Update(txnD, updateInfo)

	/*
	| hoge  | fuga  | piyo  |
	| --    | --    | --    |
	| -345  | 77    | 43    |
	| -100  | 89    | 111   |
	| 0     | 0     | 0     |
	| 2     | 4447  | 89    |
	| 10    | 45    | -999  |
	| 500   | 33    | 90    |
	| 10000 | 4     | 44    |
	| 80000 | 10    | 0     |
	*/
	st.Clear()
	st.Flush()
}
