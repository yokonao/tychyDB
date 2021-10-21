package transaction

import (
	"fmt"

	"github.com/tychyDB/storage"
	"github.com/tychyDB/util"
)

const (
	BEGIN  = 0
	UPDATE = 1
	ABORT  = 2
	COMMIT = 3
)

type Log struct {
	txnId      uint32
	lsn        uint32
	logType    uint32
	updateInfo storage.UpdateInfo
}

func newUniqueLog(lsn uint32, txnId uint32, logType uint32) *Log {
	log := &Log{}
	log.txnId = txnId
	log.lsn = lsn
	log.logType = logType
	return log
}

func (log *Log) toBytes() []byte {
	gen := util.NewGenStruct(0, storage.PageSize)
	actualLen := 3 * IntSize // len(log) is not constant
	gen.PutUInt32(log.txnId)
	gen.PutUInt32(log.lsn)
	gen.PutUInt32(log.logType)

	if log.logType == UPDATE {
		uinfoBuf := log.updateInfo.ToBytes()
		uinfoBufLen := uint32(len(uinfoBuf))
		actualLen += int(uinfoBufLen) + IntSize
		gen.PutUInt32(uinfoBufLen)
		gen.PutBytes(uinfoBufLen, uinfoBuf)
	}
	return gen.DumpBytes()[:actualLen]
}

func (log *Log) info() {
	fmt.Printf("%7d, %7d, %7d, ", log.txnId, log.lsn, log.logType)
	if log.logType == UPDATE {
		u := log.updateInfo
		fmt.Printf("%7d, %7d, %7d, %7b, %7b", u.PageIdx, u.PtrIdx, u.ColNum, u.From, u.To)
	} else {
		fmt.Printf("       ,        ,        ,         ,        ")
	}
	fmt.Print("\n")
}
