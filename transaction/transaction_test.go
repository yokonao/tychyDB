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

func createTable(t *testing.T) {
	cleanDisk(t)
	storage.Reset()

	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewTable(fm, ptb)
	tb.AddColumn("hoge", storage.IntergerType)
	tb.AddColumn("fuga", storage.IntergerType)
	tb.AddColumn("piyo", storage.IntergerType)
	tb.Add(2, -13, 89)
	tb.Add(10000, 4, 44)
	tb.Add(500, 5, 90)
	tb.Add(10, 45, -999)
	tb.Add(-345, 77, 43)
	tb.Add(-100, 89, 111)
	tb.Add(0, 0, 0)
	tb.Add(80000, 10, 0)
	tb.Flush()
}

func TestTxn(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")

	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewTableFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm, ptb)
	txn.Begin()
	updateInfo := tb.Update(2, "fuga", 33)
	txn.Update(updateInfo)
	txn.Commit()
	lm.Print()
}

func TestLogSerializeDeSerialize(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")

	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewTableFromFile(fm, ptb)

	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm, ptb)
	txn.Begin()
	updateInfo := tb.Update(2, "fuga", 33)
	txn.Update(updateInfo)
	txn.Commit()

	lm.Print()
	// test log manager serialize deserialize
	pg := lm.LogPage
	bytes := pg.ToBytes()
	newLogPage := transaction.NewLogPageFromBytes(bytes)
	newLogPage.Print()
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

func TestLogWrite(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")

	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewTableFromFile(fm, ptb)

	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm, ptb)
	txn.Begin()
	updateInfo := tb.Update(2, "fuga", 33)
	txn.Update(updateInfo)
	txn.Commit()
}

func TestLogLSN(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")
	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	tb := storage.NewTableFromFile(fm, ptb)
	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm, ptb)

	txn.Begin()
	updateInfo := tb.Update(2, "fuga", 33)
	txn.Update(updateInfo)

	updateInfo = tb.Update(2, "fuga", 3335)

	if ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx)) != 2 {
		t.Error("invalid pageLSN")
	}
	txn.Update(updateInfo)
	if ptb.GetPageLSN(storage.NewBlockId(updateInfo.PageIdx)) != 3 {
		t.Error("invalid pageLSN")
	}
	txn.Commit()
}
