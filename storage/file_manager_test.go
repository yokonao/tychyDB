package storage_test

import (
	"math/rand"
	"testing"

	"github.com/tychyDB/storage"
)

const testFName = "testFile"

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
	fileManager := storage.NewFileMgr()
	defer fileManager.Clean()
	numBlocks := (byteLen + storage.PageSize) / storage.PageSize
	for curBlkId := 0; curBlkId < numBlocks; curBlkId++ {
		curBlk := storage.NewBlockId(uint32(curBlkId), testFName)
		lower := curBlkId * storage.PageSize
		upper := min(byteLen, (curBlkId+1)*storage.PageSize)
		fileManager.Write(curBlk, token[lower:upper])
	}

	for curBlkId := 0; curBlkId < numBlocks; curBlkId++ {
		curBlk := storage.NewBlockId(uint32(curBlkId), testFName)
		lower := curBlkId * storage.PageSize
		upper := min(byteLen, (curBlkId+1)*storage.PageSize)
		buf := token[lower:upper]
		readLen, readBytes := fileManager.Read(curBlk)

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

func TestFileMgrGetLastBlock(t *testing.T) {
	byteLen := 42000
	token := make([]byte, byteLen)
	rand.Read(token)
	fileManager := storage.NewFileMgr()
	defer fileManager.Clean()
	numBlocks := (byteLen + storage.PageSize) / storage.PageSize
	for curBlkId := 0; curBlkId < numBlocks; curBlkId++ {
		curBlk := storage.NewBlockId(uint32(curBlkId), testFName)
		lower := curBlkId * storage.PageSize
		upper := min(byteLen, (curBlkId+1)*storage.PageSize)
		fileManager.Write(curBlk, token[lower:upper])
	}

	curBlkId := numBlocks - 1
	lower := curBlkId * storage.PageSize
	upper := min(byteLen, (curBlkId+1)*storage.PageSize)
	buf := token[lower:upper]

	blkId, n, lastBytes := fileManager.ReadLastBlock(testFName)
	if blkId != numBlocks-1 {
		t.Error("block id mismatch")
	}
	if n != len(buf) {
		t.Error("byte length mismatch")
	}
	for i := 0; i < n; i++ {
		if lastBytes[i] != buf[i] {
			t.Error("byte mismatch")
		}
	}
}
