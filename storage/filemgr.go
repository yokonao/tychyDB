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

func (fm *FileMgr) write(blk BlockId, bytes []byte) {
	file, err := os.OpenFile(fm.basePath+"testfile", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(int64(blk.blockNum)*fm.blockSize, 0)
	if err != nil {
		panic(err)
	}

	_, err = file.Write(bytes)
	if err != nil {
		panic(err)
	}
}

func (fm *FileMgr) read(blk BlockId) (int, []byte) {
	file, err := os.Open(fm.basePath + "testfile")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(int64(blk.blockNum)*fm.blockSize, 0)

	if err != nil {
		panic(err)
	}

	buf := make([]byte, PageSize)
	n, err := file.Read(buf)
	if n == 0 {
		return n, buf
	}
	if err != nil {
		panic(err)
	}
	return n, buf
}
