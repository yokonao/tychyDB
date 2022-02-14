package storage_test

import (
	"fmt"
	"testing"

	"github.com/tychyDB/storage"
	"github.com/tychyDB/util"
)

func TestStorageEasy(t *testing.T) {
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
	st.Add(10000, 4, 44)
	st.Add(-345, 77, 43)
	fm.Clean()
}
func TestStorage(t *testing.T) {
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
	res, err := st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	st.Flush()
	res, err = st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	fm.Clean()
}

func TestStorageChar(t *testing.T) {
	storage.ResetBlockId()
	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	countryTable := storage.NewStorage(fm, ptb)
	countryTable.AddColumn("name", storage.CharType(13))
	countryTable.AddColumn("continent", storage.CharType(15))
	countryTable.Add("Japan", "Asia")
	countryTable.Add("China", "Asia")
	countryTable.Add("United States", "North America")
	countryTable.Add("Russia", "Eurasia")
	countryTable.Add("Brazil", "South America")
	countryTable.Add("Nigeria", "Africa")

	res, err := countryTable.Select(false, "name", "continent")
	if err != nil {
		t.Error("failure select")
	}

	if res[0][1].(string) != "China" {
		t.Errorf("expected: China, actual: %s\n", res[0][1].(string))
	}
	if res[0][2].(string) != "Russia" {
		t.Errorf("expected: Russia, actual: %s\n", res[0][2].(string))
	}
	if res[0][3].(string) != "Brazil" {
		t.Errorf("expected: Brazil, actual: %s\n", res[0][3].(string))
	}

	countryTable.Flush()
	res, err = countryTable.Select(true, "continent", "name")
	if err != nil {
		t.Error("failure select")
	}
	if res[0][0].(string) != "Asia" {
		t.Errorf("expected: Asia, actual: %s\n", res[0][0].(string))
	}
	if res[1][1].(string) != "China" {
		t.Errorf("expected: China, actual: %s\n", res[1][1].(string))
	}
	if res[1][2].(string) != "Russia" {
		t.Errorf("expected: Russia, actual: %s\n", res[1][2].(string))
	}
	if res[1][5].(string) != "United States" {
		t.Errorf("expected: United States, actual: %s\n", res[1][3].(string))
	}
	fm.Clean()
}

func TestStorageMixed(t *testing.T) {
	storage.CreateStorageWithChar()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)
	_, err := st.Select(false, "hoge", "fuga", "piyo", "hogefuga", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	fm.Clean()
}

func TestStorageRestore(t *testing.T) {
	storage.CreateStorage()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)

	res, err := st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	st.Flush()
	res, err = st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	fm.Clean()
}

func TestUpdate(t *testing.T) {
	storage.CreateStorage()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	st.Update(2, "fuga", 33)
	st.Update(10, "fuga", 44)
	st.Update(10, "piyo", 4)

	res, err := st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	if res[3][3].(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", res[3][3])
	}
	if res[1][4].(int32) != 44 {
		t.Errorf("expected: 44, actual: %d", res[3][3])
	}

	if res[2][4].(int32) != 4 {
		t.Errorf("expected: 4, actual: %d", res[3][3])
	}
	res, err = st.Select(false, "hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	st.Update(2, "fuga", 333)
	if res[3][3].(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", res[3][3])
	}
	fm.Clean()
}

func TestUpdateIdempotent(t *testing.T) {
	storage.CreateStorage()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	st.Update(2, "fuga", 33)
	st.Update(2, "fuga", 33)
	st.Update(10, "fuga", 44)
	st.Update(10, "fuga", 44)
	st.Update(10, "piyo", 4)
	st.Update(10, "piyo", 4)

	res, err := st.Select(false, "hoge", "fuga", "piyo", "fuga")
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
	if res[3][3].(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", res[3][3])
	}
	if res[1][4].(int32) != 44 {
		t.Errorf("expected: 44, actual: %d", res[1][4])
	}

	if res[2][4].(int32) != 4 {
		t.Errorf("expected: 4, actual: %d", res[2][4])
	}
	st.Update(2, "fuga", 777)
	res, err = st.Select(false, "hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[3][3].(int32) != 777 {
		t.Errorf("expected: 777, actual: %d", res[3][3])
	}
	fm.Clean()
}
func TestUpdateFromInfo(t *testing.T) {
	storage.CreateStorage()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	ui := st.Update(10, "fuga", 44)
	gen := util.NewGenStruct(0, uint32(len(ui.To)))
	gen.PutUInt32(555)
	ui.To = gen.DumpBytes()
	st.UpdateFromInfo(&ui)

	res, err := st.Select(false, "hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[1][4].(int32) != 555 {
		t.Errorf("expected: 555, actual: %d", res[1][4])
	}

	fm.Clean()
}

func TestUpdateFromInfoMixed(t *testing.T) {
	storage.CreateStorageWithChar()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	ui := st.Update(10, "fuga", 44)
	gen := util.NewGenStruct(0, uint32(len(ui.To)))
	gen.PutUInt32(555)
	ui.To = gen.DumpBytes()
	st.UpdateFromInfo(&ui)

	ui = st.Update(500, "hogefuga", "before")
	gen = util.NewGenStruct(0, 14)
	gen.PutStringWithSize("after", 10)
	ui.To = gen.DumpBytes()
	st.UpdateFromInfo(&ui)

	res, err := st.Select(false, "hoge", "fuga", "piyo", "hogefuga")
	if err != nil {
		t.Error("failure select")
	}
	if res[3][5].(string) != "after" {
		fmt.Println(len(res[3][5].(string)))
		t.Errorf("expected: after, actual: %v", res[3][5].(string))
	}

	fm.Clean()
}
