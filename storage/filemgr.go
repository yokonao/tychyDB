package storage

import (
	"errors"
	"os"
)

var (
	ErrFileOpen      = errors.New("I/O error while opening file")
	ErrFileSeek      = errors.New("I/O error while seeking file")
	ErrFileRead      = errors.New("I/O error while reading file")
	ErrFileReadShort = errors.New("Read too short")
	ErrFileWrite     = errors.New("I/O error while writing file")
)

type FileMgr struct {
	baseDir   string
	blockSize int64
	isNew     bool
	// mu        sync.Mutex
	// openFiles map[string]*os.File
}

func NewFileMgr() *FileMgr {
	diskDir := os.Getenv("DISK")
	fm := &FileMgr{}
	fm.baseDir = diskDir
	fm.blockSize = PageSize
	_, err := os.Stat(fm.baseDir)
	fm.isNew = err != nil
	if fm.isNew {
		os.Mkdir(fm.baseDir, 0777)
	}
	return fm
}

func (fm *FileMgr) Clean() {
	err := os.RemoveAll(fm.baseDir)
	if err != nil {
		panic(err)
	}
}

func (fm *FileMgr) Write(blk BlockId, bytes []byte) {
	file, err := os.OpenFile(fm.baseDir+blk.fileName, os.O_WRONLY|os.O_CREATE, 0644)
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
	file, err := os.Open(fm.baseDir + blk.fileName)
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

func (fm *FileMgr) ReadLastBlock(fileName string) (int, int, []byte) {
	curBlkId := 0
	lastBuf := make([]byte, PageSize)
	lastN := 0
	for {
		n, buf := fm.Read(NewBlockId(uint32(curBlkId), fileName))
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
