package transaction_test

import (
	"os"
	"testing"

	"github.com/tychyDB/storage"
	"github.com/tychyDB/transaction"
)

func cleanDisk(t *testing.T) {
	diskDir := os.Getenv("DISK")
	os.Remove(diskDir + "/testfile")
	os.Remove(diskDir + "/logfile")
}

func createStorage(t *testing.T) {
	cleanDisk(t)
	storage.ResetBlockId()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorage(fm, ptb)
	st.AddColumn("hoge", storage.IntergerType)
	st.AddColumn("fuga", storage.IntergerType)
	st.AddColumn("piyo", storage.IntergerType)
	st.Add(2, -13, 89)
	st.Add(10000, 4, 44)
	st.Add(500, 5, 90)
	st.Add(10, 45, -999)
	st.Add(-345, 77, 43)
	st.Add(-100, 89, 111)
	st.Add(0, 0, 0)
	st.Add(80000, 10, 0)
	st.Flush()
}

func TestTxn(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)

	txn := transaction.NewTransaction()
	rm.Begin(txn)
	updateInfo := tb.Update(2, "fuga", 33)
	rm.Update(txn, updateInfo)
	rm.Commit(txn)
	fm.Clean()
}

func TestLogSerializeDeSerialize(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)
	txn := transaction.NewTransaction()

	rm.Begin(txn)
	updateInfo := st.Update(2, "fuga", 33)
	rm.Update(txn, updateInfo)
	rm.Commit(txn)

	// test log manager serialize deserialize
	pg := lm.LogPage
	bytes := pg.ToBytes()
	newLogPage := transaction.NewLogPageFromBytes(bytes)
	newBytes := newLogPage.ToBytes()

	if len(bytes) != len(newBytes) {
		t.Error("byte length mismatch")
	}
	for i := 0; i < len(bytes); i++ {
		if bytes[i] != newBytes[i] {
			t.Error("byte mismatch")
		}
	}
	fm.Clean()
}

func TestLogLSN(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()
	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)
	txn := transaction.NewTransaction()

	rm.Begin(txn)
	updateInfo := st.Update(2, "fuga", 33)
	rm.Update(txn, updateInfo)

	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 2 {
		t.Errorf("invalid pageLSN expect %d got %d", 2, val)
	}
	st.Flush()
	storage.ResetBlockId()
	st = storage.NewStorageFromFile(fm, ptb)
	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 2 {
		t.Errorf("invalid pageLSN expect %d got %d", 2, val)
	}
	rm.Commit(txn)
	fm.Clean()
}

func TestLogLSNConcurrently(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()
	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)

	txnA := transaction.NewTransaction()
	txnB := transaction.NewTransaction()

	rm.Begin(txnA)
	updateInfo := st.Update(2, "fuga", 33)
	rm.Update(txnA, updateInfo)

	rm.Begin(txnB)
	updateInfo = st.Update(2, "fuga", 3335)

	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 2 {
		t.Errorf("invalid pageLSN expect %d got %d", 2, val)
	}
	rm.Update(txnA, updateInfo)
	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 4 {
		t.Errorf("invalid pageLSN expect %d got %d", 4, val)
	}
	rm.Abort(txnB)
	rm.Commit(txnA)

	st.Flush()
	storage.ResetBlockId()
	st = storage.NewStorageFromFile(fm, ptb)
	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 4 {
		t.Errorf("invalid pageLSN expect %d got %d", 4, val)
	}
	fm.Clean()
}

func TestLogIterator(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()
	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)

	txnA := transaction.NewTransaction()
	txnB := transaction.NewTransaction()

	rm.Begin(txnA)
	rm.Begin(txnB)
	rm.Abort(txnB)
	rm.Commit(txnA)

	logIter := transaction.NewLogIter(lm, 0)
	log, _ := logIter.Next()
	if val := log.LSN(); val != 1 {
		t.Errorf("invalid LSN expect %d got %d", 1, val)
	}

	if val := log.TxnID(); val != 0 {
		t.Errorf("invalid TxnID expect %d got %d", 0, val)
	}

	if val := log.LogType(); val != 0 {
		t.Errorf("invalid LogType expect %d got %d", 0, val)
	}

	log, _ = logIter.Next()
	if val := log.LSN(); val != 2 {
		t.Errorf("invalid LSN expect %d got %d", 2, val)
	}
	if val := log.TxnID(); val != 1 {
		t.Errorf("invalid TxnID expect %d got %d", 1, val)
	}
	if val := log.LogType(); val != 0 {
		t.Errorf("invalid LogType expect %d got %d", 0, val)
	}

	log, _ = logIter.Next()
	if val := log.LSN(); val != 3 {
		t.Errorf("invalid LSN expect %d got %d", 3, val)
	}
	if val := log.TxnID(); val != 1 {
		t.Errorf("invalid TxnID expect %d got %d", 1, val)
	}
	if val := log.LogType(); val != 2 {
		t.Errorf("invalid LogType expect %d got %d", 2, val)
	}

	log, _ = logIter.Next()
	if val := log.LSN(); val != 4 {
		t.Errorf("invalid LSN expect %d got %d", 4, val)
	}
	if val := log.TxnID(); val != 0 {
		t.Errorf("invalid TxnID expect %d got %d", 0, val)
	}
	if val := log.LogType(); val != 3 {
		t.Errorf("invalid LogType expect %d got %d", 3, val)
	}

	log, err := logIter.Next()
	if err != transaction.ErrOutOfBounds {
		t.Errorf("expected ErrOutOfBounds got %v", err)
	}
	fm.Clean()
}

func TestUpdateFromLog(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()
	logfm := storage.NewFileMgr()
	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)
	txn := transaction.NewTransaction()

	rm.Begin(txn)
	updateInfo := st.Update(2, "fuga", 33)
	rm.Update(txn, updateInfo)

	updateInfo = st.Update(2, "fuga", 3335)

	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 2 {
		t.Errorf("invalid pageLSN expect %d got %d", 2, val)
	}
	rm.Update(txn, updateInfo)
	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 3 {
		t.Errorf("invalid pageLSN expect %d got %d", 3, val)
	}

	st.Flush()
	storage.ResetBlockId()
	st = storage.NewStorageFromFile(fm, ptb)
	if val := ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)); val != 3 {
		t.Errorf("invalid pageLSN expect %d got %d", 3, val)
	}
	rm.Commit(txn)
	fm.Clean()
}
