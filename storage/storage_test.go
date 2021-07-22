package storage_test

import (
	"github.com/tychyDB/storage"
	"testing"
)

func TestStorage(t *testing.T){
	s := storage.NewStorage()
	s.AddColumn("hoge")
	s.AddColumn("fuga")
	s.AddColumn("piyo")
	s.Add(2, -13, 89)
	s.Add(10000, 4, 44)
	s.Add(500, 5, 90)
	s.Add(10, 45, -999)
	s.Add(-100, 89, 111)
	s.Select("hoge", "fuga", "piyo", "fuga")
}
