package storage

import (
	"os"
)

type FileMgr struct {
	basePath  string
	blockSize int64
}

func newFileMgr() *FileMgr {
	curDir, err := os.Getwd()
	if err != nil {
		panic("failure for getting current director path")
	}
	return &FileMgr{
		basePath:  curDir + "/disk/",
		blockSize: PageSize,
	}
}

func (fm *FileMgr) write(blk *BlockId, pg *Page) {
	file, err := os.OpenFile(fm.basePath+"testfile", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(blk.blockNum*fm.blockSize, 0)
	if err != nil {
		panic(err)
	}

	_, err = file.Write(pg.toBytes())
	if err != nil {
		panic(err)
	}
}

func (fm *FileMgr) read(blk *BlockId) (int, Page) {
	file, err := os.Open(fm.basePath + "testfile")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(blk.blockNum*fm.blockSize, 0)

	if err != nil {
		panic(err)
	}

	buf := make([]byte, PageSize)
	n, err := file.Read(buf)
	if n == 0 {
		return n, newPage()
	}
	if err != nil {
		panic(err)
	}
	pg := newPageFromBytes(buf)
	return n, pg
}
