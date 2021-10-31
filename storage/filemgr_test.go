package storage_test

import (
	"math/rand"
	"testing"

	"github.com/tychyDB/storage"
)

func min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func TestFileMgr(t *testing.T) {
	byteLen := 42000
	token := make([]byte, byteLen)
	rand.Read(token)
	fm := storage.NewFileMgr("testFileMgr")
	defer fm.Clean()
	numBlocks := (byteLen + storage.PageSize) / storage.PageSize
	for curBlkId := 0; curBlkId < numBlocks; curBlkId++ {
		curBlk := storage.NewBlockId(uint32(curBlkId))
		lower := curBlkId * storage.PageSize
		upper := min(byteLen, (curBlkId+1)*storage.PageSize)
		fm.Write(curBlk, token[lower:upper])
	}

	for curBlkId := 0; curBlkId < numBlocks; curBlkId++ {
		curBlk := storage.NewBlockId(uint32(curBlkId))
		lower := curBlkId * storage.PageSize
		upper := min(byteLen, (curBlkId+1)*storage.PageSize)
		buf := token[lower:upper]
		readLen, readBytes := fm.Read(curBlk)

		if readLen != len(buf) {
			t.Error("byte length mismatch")
		}

		for i := 0; i < readLen; i++ {
			if readBytes[i] != buf[i] {
				t.Error("byte mismatch")
			}
		}
	}
}
