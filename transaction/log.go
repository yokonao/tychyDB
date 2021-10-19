package transaction

import (
	"encoding/binary"
	"fmt"

	"github.com/tychyDB/storage"
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
	buf := make([]byte, 4*IntSize)
	binary.BigEndian.PutUint32(buf[IntSize:2*IntSize], log.txnId)
	binary.BigEndian.PutUint32(buf[2*IntSize:3*IntSize], log.lsn)
	binary.BigEndian.PutUint32(buf[3*IntSize:4*IntSize], log.logType)
	if log.logType == UPDATE {
		uinfoBuf := log.updateInfo.ToBytes()
		buf = append(buf, uinfoBuf...)
	}
	binary.BigEndian.PutUint32(buf[:IntSize], uint32(len(buf)))
	return buf
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
