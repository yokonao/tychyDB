package storage

import (
	"errors"
	"os"
)

type FileMgr struct {
	fileName  string
	basePath  string
	blockSize int64
}

func NewFileMgr(fileName string) *FileMgr {
	diskDir := os.Getenv("DISK")
	return &FileMgr{
		fileName:  fileName,
		basePath:  diskDir,
		blockSize: PageSize,
	}
}

func (fm *FileMgr) Clean() {
	diskDir := os.Getenv("DISK")
	os.Remove(diskDir + fm.fileName)
}

func (fm *FileMgr) Write(blk BlockId, bytes []byte) {
	file, err := os.OpenFile(fm.basePath+fm.fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	_, err = file.Seek(int64(blk.BlockNum)*fm.blockSize, 0)
	if err != nil {
		panic(err)
	}

	_, err = file.Write(bytes)
	if err != nil {
		panic(err)
	}
}

func (fm *FileMgr) Read(blk BlockId) (int, []byte) {
	file, err := os.Open(fm.basePath + fm.fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(int64(blk.BlockNum)*fm.blockSize, 0)

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

func (fm *FileMgr) ReadLastBlock() (int, int, []byte) {
	curBlkId := 0
	lastBuf := make([]byte, PageSize)
	lastN := 0
	for {
		n, buf := fm.Read(NewBlockId(uint32(curBlkId)))
		if n == 0 {
			if curBlkId == 0 {
				panic(errors.New("cannot get last block"))
			}
			return curBlkId - 1, lastN, lastBuf
		} else if n != PageSize {
			return curBlkId, n, buf
		}

		copy(lastBuf, buf)
		lastN = n
		curBlkId++
	}
}
