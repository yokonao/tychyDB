package storage

func CreateStorage() {
	fm := NewFileMgr()
	bm := NewBufferMgr(fm)
	ptb := NewPageTable(bm)

	st := NewStorage(fm, ptb)
	st.AddColumn("hoge", IntergerType)
	st.AddColumn("fuga", IntergerType)
	st.AddColumn("piyo", IntergerType)
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

func CreateStorageWithChar() {
	fm := NewFileMgr()
	bm := NewBufferMgr(fm)
	ptb := NewPageTable(bm)

	st := NewStorage(fm, ptb)
	st.AddColumn("hoge", IntergerType)
	st.AddColumn("fuga", IntergerType)
	st.AddColumn("hogefuga", CharType(10))
	st.AddColumn("piyo", IntergerType)
	st.Add(2, -13, "pika", 89)
	st.Add(10000, 4, "pika", 44)
	st.Add(500, 5, "pokemon", 90)
	st.Add(10, 45, "luckey", -999)
	st.Add(-345, 77, "767", 43)
	st.Add(-100, 89, "r", 111)
	st.Add(0, 0, "", 0)
	st.Add(80000, 10, "bigbigbigA", 0)
	st.Flush()
}
