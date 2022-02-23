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
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 2)

	st.Flush()
	st = storage.NewStorageFromFile(fm, ptb)
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 2)

	rm.Commit(txn)
}

func TestLogLSNConcurrently(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
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

	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 2)

	rm.Update(txnA, updateInfo)

	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 4)
	rm.Abort(txnB)
	rm.Commit(txnA)

	st.Flush()
	st = storage.NewStorageFromFile(fm, ptb)
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 4)
}

func TestLogIterator(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
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
	assert.EqualUInt32(t, uint32(log.TxnID()), 0)
	assert.EqualUInt32(t, log.LogType(), 0)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 2)
	assert.EqualUInt32(t, uint32(log.TxnID()), 1)
	assert.EqualUInt32(t, log.LogType(), 0)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 3)
	assert.EqualUInt32(t, uint32(log.TxnID()), 1)
	assert.EqualUInt32(t, log.LogType(), 2)

	log, _ = logIter.Next()
	assert.EqualUInt32(t, log.LSN(), 4)
	assert.EqualUInt32(t, uint32(log.TxnID()), 0)
	assert.EqualUInt32(t, log.LogType(), 3)

	log, err := logIter.Next()
	if err != transaction.ErrOutOfBounds {
		t.Errorf("expected ErrOutOfBounds got %v", err)
	}
}

func TestUpdateFromLog(t *testing.T) {
	transaction.UniqueTxnId = 0
	createStorage(t)
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
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 2)

	rm.Update(txn, updateInfo)
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 3)

	st.Flush()
	st = storage.NewStorageFromFile(fm, ptb)
	assert.EqualUInt32(t, ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx, storage.StorageFile)), 3)

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
	assert.EqualInt32(t, res[1][3].(int32), -13)
	assert.EqualInt32(t, res[1][5].(int32), 5)
	rm.LogRedo(&st)
	res, _ = st.Select(false, "hoge", "fuga")
	assert.EqualInt32(t, res[1][3].(int32), 3337)
	assert.EqualInt32(t, res[1][5].(int32), 33)
}

func TestRedoFromLogFile(t *testing.T) {
	transaction.UniqueTxnId = 0
	transaction.CreateLogFile()

	fm := storage.NewFileMgr()
	defer fm.Clean()

	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	lm := transaction.NewLogMgrFromFile(*fm)
	rm := transaction.NewRecoveryMgr(lm, ptb)

	res, _ := st.Select(false, "hoge", "fuga", "piyo")
	assert.EqualInt32(t, res[1][3].(int32), -13)
	assert.EqualInt32(t, res[1][5].(int32), 5)
	rm.LogRedo(&st)
	res, _ = st.Select(false, "hoge", "fuga", "piyo")
	assert.EqualInt32(t, res[1][3].(int32), 4447)
	assert.EqualInt32(t, res[1][5].(int32), 33)
}
