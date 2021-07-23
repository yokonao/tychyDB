package storage

import (
	"os"
)

type FileMgr struct {
	blockSize int64
}

func newFileMgr() *FileMgr {
	return &FileMgr{
		blockSize: PageSize,
	}
}

func (fm *FileMgr) read(blk *BlockId, pg *Page) int {
	// PageにBlockから読み出す

	file, err := os.Open(blk.fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Seek(blk.blockNum*fm.blockSize, 0)
	buf := make([]byte, PageSize)
	n, err := file.Read(buf)
	if n == 0 {
		return n
	}
	if err != nil {
		panic(err)
	}
	pg.setBytes(buf)
	return n
}

func (fm *FileMgr) write(blk *BlockId, pg *Page) {
	// PageをBlockへ書き出す

	file, err := os.Create(blk.fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(blk.blockNum*fm.blockSize, 0)
	if err != nil {
		panic(err)
	}
	_, err = file.Write(pg.bb)
	if err != nil {
		panic(err)
	}
}

func fileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	return err == nil
}

func (fm *FileMgr) getFile(fileName string) *os.File {
	file, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	return file
}
