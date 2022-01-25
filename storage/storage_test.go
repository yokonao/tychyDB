package storage_test

import (
	"strings"
	"testing"

	"github.com/tychyDB/storage"
)

func createStorage(t *testing.T) {
	storage.Reset()
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

	_, err := st.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
}

func TestDebug(t *testing.T) {
	createStorage(t)
}

func TestStorageEasy(t *testing.T) {
	storage.Reset()
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
}
func TestStorage(t *testing.T) {
	storage.Reset()

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
	res, err := st.Select("hoge", "fuga", "piyo", "fuga")
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
	res, err = st.Select("hoge", "fuga", "piyo", "fuga")
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

func TestStorageChar(t *testing.T) {
	storage.Reset()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	countryTable := storage.NewStorage(fm, ptb)
	countryTable.AddColumn("name", storage.CharType(10))
	countryTable.AddColumn("continent", storage.CharType(15))
	countryTable.Add("Japan", "Asia")
	countryTable.Add("China", "Asia")
	countryTable.Add("United States", "North America")
	countryTable.Add("Russia", "Eurasia")
	countryTable.Add("Brazil", "South America")
	countryTable.Add("Nigeria", "Africa")

	res, err := countryTable.Select("name", "continent")
	if err != nil {
		t.Error("failure select")
	}

	if !strings.HasPrefix(res[0][1].(string), "China") {
		t.Errorf("expected: China, actual: %s\n", res[0][1].(string))
	}
	if !strings.HasPrefix(res[0][2].(string), "Japan") {
		t.Errorf("expected: Japan, actual: %s\n", res[0][2].(string))
	}
	if !strings.HasPrefix(res[0][3].(string), "Nigeria") {
		t.Errorf("expected: Nigeria, actual: %s\n", res[0][3].(string))
	}

	countryTable.Flush()
	res, err = countryTable.Select("continent", "name")
	if err != nil {
		t.Error("failure select")
	}
	if !strings.HasPrefix(res[0][0].(string), "South America") {
		t.Errorf("expected: South America, actual: %s\n", res[0][0].(string))
	}
	if !strings.HasPrefix(res[1][1].(string), "China") {
		t.Errorf("expected: China, actual: %s\n", res[1][1].(string))
	}
	if !strings.HasPrefix(res[1][2].(string), "Japan") {
		t.Errorf("expected: Japan, actual: %s\n", res[1][2].(string))
	}
	if !strings.HasPrefix(res[1][3].(string), "Nigeria") {
		t.Errorf("expected: Nigeria, actual: %s\n", res[1][3].(string))
	}
}

func TestStorageRestore(t *testing.T) {
	createStorage(t)
	storage.Reset()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb)

	res, err := st.Select("hoge", "fuga", "piyo", "fuga")
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
	res, err = st.Select("hoge", "fuga", "piyo", "fuga")
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

func TestUpdate(t *testing.T) {
	createStorage(t)
	storage.Reset()

	fm := storage.NewFileMgr()
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	st.Update(2, "fuga", 33)
	st.Update(10, "fuga", 44)
	st.Update(10, "piyo", 4)

	res, err := st.Select("hoge", "fuga", "piyo", "fuga")
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
	res, err = st.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	st.Update(2, "fuga", 333)
	if res[3][3].(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", res[3][3])
	}
}

func TestUpdateIdempotent(t *testing.T) {
	createStorage(t)
	storage.Reset()

	fm := storage.NewFileMgr("testfile")
	bm := storage.NewBufferMgr(fm)
	ptb := storage.NewPageTable(bm)
	st := storage.NewStorageFromFile(fm, ptb) // hogeがプライマリー

	st.Update(2, "fuga", 33)
	st.Update(2, "fuga", 33)
	st.Update(10, "fuga", 44)
	st.Update(10, "fuga", 44)
	st.Update(10, "piyo", 4)
	st.Update(10, "piyo", 4)

	res, err := st.Select("hoge", "fuga", "piyo", "fuga")
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
	res, err = st.Select("hoge", "fuga", "piyo", "fuga")
	if err != nil {
		t.Error("failure select")
	}
	st.Update(2, "fuga", 333)
	if res[3][3].(int32) != 33 {
		t.Errorf("expected: 33, actual: %d", res[3][3])
	}
}
