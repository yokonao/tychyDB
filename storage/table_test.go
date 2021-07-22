package storage_test

import (
	"github.com/tychyDB/storage"
	"testing"
)

func TestStorage(t *testing.T){
	tb := storage.NewTable()
	tb.AddColumn("hoge")
	tb.AddColumn("fuga")
	tb.AddColumn("piyo")
	// tb.Add(2, -13, 89)
	// tb.Add(10000, 4, 44)
	// tb.Add(500, 5, 90)
	// tb.Add(10, 45, -999)
	// tb.Add(-100, 89, 111)
	tb.Read()
	tb.Select("hoge", "fuga", "piyo", "fuga")
	// tb.Write()
}
