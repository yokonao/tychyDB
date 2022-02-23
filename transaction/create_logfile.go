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
	logManager := NewLogMgr(*logfm)
	recoveryManager := NewRecoveryMgr(logManager, ptb)
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
	recoveryManager.Begin(txnA)
	updateInfo := st.Update(500, "fuga", 33)
	recoveryManager.Update(txnA, updateInfo)
	updateInfo = st.Update(2, "fuga", 3337)
	recoveryManager.Update(txnA, updateInfo)
	recoveryManager.Commit(txnA)

	txnB := NewTransaction()
	txnC := NewTransaction()
	recoveryManager.Begin(txnB)
	updateInfo = st.Update(2, "fuga", 44)
	recoveryManager.Update(txnB, updateInfo)

	recoveryManager.Begin(txnC)
	updateInfo = st.Update(2, "fuga", 4447)
	recoveryManager.Update(txnB, updateInfo)
	updateInfo = st.Update(2, "fuga", 5557)
	recoveryManager.Update(txnC, updateInfo)
	recoveryManager.Abort(txnC)
	recoveryManager.Commit(txnB)

	txnD := NewTransaction()
	recoveryManager.Begin(txnD)
	updateInfo = st.Update(2, "fuga", 66)
	recoveryManager.Update(txnD, updateInfo)
	updateInfo = st.Update(2, "fuga", 6667)
	recoveryManager.Update(txnD, updateInfo)

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
