package storage_test

import (
	"os"
	"testing"

	"github.com/tychyDB/storage"
)

func TestStorage(t *testing.T) {
	// clean up disk directory
	curDir, err := os.Getwd()
	if err != nil {
		t.Fatal("failure for getting current directory path")
	}
	diskDir := curDir + "/disk"
	os.Remove(diskDir + "/testfile")

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
	res, err := tb.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[0][0].(int32) != -345 {
		t.Errorf("expected: -345, actual: %d", res[0][0])
	}
	if res[1][1].(int32) != 89 {
		t.Errorf("expected: 89, actual: %d", res[1][1])
	}
	if res[2][2].(int32) != 0 {
		t.Errorf("expected: 0, actual: %d", res[2][2])
	}
	if res[3][3].(int32) != -13 {
		t.Errorf("expected: -13, actual: %d", res[3][3])
	}

	tb.Write()
	tb.Read()
	res, err = tb.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[0][0].(int32) != -345 {
		t.Errorf("expected: -345, actual: %d", res[0][0])
	}
	if res[1][1].(int32) != 89 {
		t.Errorf("expected: 89, actual: %d", res[1][1])
	}
	if res[2][2].(int32) != 0 {
		t.Errorf("expected: 0, actual: %d", res[2][2])
	}
	if res[3][3].(int32) != -13 {
		t.Errorf("expected: -13, actual: %d", res[3][3])
	}
}
