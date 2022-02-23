package transaction_test

import (
	"os"
	"testing"

	"github.com/tychyDB/assert"
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
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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
}

func TestLogSerializeDeSerialize(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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
}

func TestLogLSN(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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
}

func TestLogLSNConcurrently(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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
}

func TestLogIterator(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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
	assert.EqualUInt32(t, log.LSN(), 1)
	assert.EqualUInt32(t, log.TxnID(), 0)
	assert.EqualUInt32(t, log.LogType(), 0)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 2)
	assert.EqualUInt32(t, log.TxnID(), 1)
	assert.EqualUInt32(t, log.LogType(), 0)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 3)
	assert.EqualUInt32(t, log.TxnID(), 1)
	assert.EqualUInt32(t, log.LogType(), 2)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 4)
	assert.EqualUInt32(t, log.TxnID(), 0)
	assert.EqualUInt32(t, log.LogType(), 3)

	log, err := logIter.Next()
	if err != transaction.ErrOutOfBounds {
		t.Errorf("expected ErrOutOfBounds got %v", err)
	}
}

func TestUpdateFromLog(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
	storage.ResetBlockId()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

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

	st.Select(false)
}

func TestRedoFromLog(t *testing.T) {
	transaction.UniqueTxnId = 0
	storage.CreateStorage()

	logfm := storage.NewFileMgr()
	defer logfm.Clean()
	fm := storage.NewFileMgr()
	defer fm.Clean()

	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	rm := transaction.NewRecoveryMgr(lm, ptb)
	txn := transaction.NewTransaction()

	rm.Begin(txn)
	updateInfo := st.Update(500, "fuga", 33)
	rm.Update(txn, updateInfo)

	updateInfo = st.Update(2, "fuga", 3337)
	rm.Update(txn, updateInfo)

	rm.Commit(txn)

	res, _ := st.Select(false, "hoge", "fuga")
	if val := res[1][3]; val.(int32) != 3337 {
		t.Errorf("expected: 3337, actual: %d", val)
	}
	if val := res[1][5]; val.(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", val)
	}
	st.Clear()
	res, _ = st.Select(false, "hoge", "fuga")
	if val := res[1][3]; val.(int32) != -13 {
		t.Errorf("expected: -13, actual: %d", val)
	}
	if val := res[1][5]; val.(int32) != 5 {
		t.Errorf("expected: 5, actual: %d", val)
	}
	rm.LogRedo(&st)
	res, _ = st.Select(false, "hoge", "fuga")
	if val := res[1][3]; val.(int32) != 3337 {
		t.Errorf("expected: 3337, actual: %d", val)
	}
	if val := res[1][5]; val.(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", val)
	}
}
