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

	tb := storage.NewTable()
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

	_, err := tb.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
}

func TestTxn(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")

	tb := storage.NewTableFromFIle()
	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm)
	txn.Begin()
	updateInfo := tb.Update("hoge", 2, "fuga", 33)
	txn.Update(updateInfo)
	txn.Commit()
	lm.Print()
}

func TestLogSerializeDeSerialize(t *testing.T) {
	createTable(t)
	storage.Reset()
	logfm := storage.NewFileMgr("logfile")

	tb := storage.NewTableFromFIle()
	lm := transaction.NewLogMgr(*logfm)
	txn := transaction.NewTransaction(lm)
	txn.Begin()
	updateInfo := tb.Update("hoge", 2, "fuga", 33)
	txn.Update(updateInfo)
	txn.Commit()

	lm.Print()
	// test log manager serialize deserialize
	bytes := lm.LogPage.ToBytes()
	newlm := transaction.NewLogPageFromBytes(bytes)
	newlm.Print()
}
