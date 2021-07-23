package storage_test

import (
	"os"
	"testing"
	"github.com/tychyDB/storage"
)

func TestStorage(t *testing.T) {
	file, err := os.Create("testfile")
	if err != nil {
		panic(err)
	}
	file.Close()
	tb := storage.NewTable()
	tb.AddColumn("hoge")
	tb.AddColumn("fuga")
	tb.AddColumn("piyo")
	tb.Add(2, -13, 89)
	tb.Add(10000, 4, 44)
	tb.Add(500, 5, 90)
	tb.Add(10, 45, -999)
	tb.Add(-100, 89, 111)
	tb.Add(-100, 89, 111)
	res, err := tb.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[0][0] != 2 {
		t.Errorf("expected: 2, actual: %d", res[0][0])
	}
	if res[1][1] != 4 {
		t.Errorf("expected: 4, actual: %d", res[1][1])
	}
	if res[2][2] != 90 {
		t.Errorf("expected: 90, actual: %d", res[2][2])
	}
	if res[3][3] != 45 {
		t.Errorf("expected: 45, actual: %d", res[3][3])
	}

	tb.Write()
	tb.Read()
	res, err = tb.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[0][0] != 2 {
		t.Errorf("expected: 2, actual: %d", res[0][0])
	}
	if res[1][1] != 4 {
		t.Errorf("expected: 4, actual: %d", res[1][1])
	}
	if res[2][2] != 90 {
		t.Errorf("expected: 90, actual: %d", res[2][2])
	}
	if res[3][3] != 45 {
		t.Errorf("expected: 45, actual: %d", res[3][3])
	}
}
