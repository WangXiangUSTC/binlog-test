package binlog

import (
	"log"
	"fmt"
	"strconv"
	"hash/crc32"
	"encoding/binary"

)

var MagicNumber uint32 = 471532804
var crcTable = crc32.MakeTable(crc32.Castagnoli)

type MutationType int32

const (
	Insert MutationType = 0
	Update MutationType = 1
	DeleteID MutationType = 2
	DeletePK MutationType = 3
	DeleteRow MutationType = 3
)

type TableMutation struct {
	TableID      int64
	InsertedRows [][]byte
	UpdatedRows  [][]byte
	DeleteIDs    []int64
	DeletePKs    [][]byte
	DeleteRows   [][]byte
	Sequence     []MutationType
}

type PreWriteValue struct {
	SchemaVersion int64
	Mutations     []*TableMutation
}

type BinlogType int32

const (
	PreWrite BinlogType = 0
	Commit   BinlogType = 1
	Rollback BinlogType = 2
	PreDDL   BinlogType = 3
	PostDDL  BinlogType = 4
)

func NewBinlogType(i int) BinlogType {
	switch i {
	case 0:
		return PreWrite
	case 1:
		return Commit
	case 2:
		return Rollback
	case 3:
		return PreDDL
	case 4:
		return PostDDL
	}

	return PreWrite
}

type Binlog struct {
	Tp            BinlogType
	StartTs       uint64
	CommitTs      uint64
	PreWriteKey   []byte
	PreWriteValue []byte
	DDLQuery      []byte
	DDLJobID      uint64
}

// | magic word | binlog type | start_ts | commit_ts | prewrite_key length | prewrite_key |
// | 4 byte     | 1 byte      | 8 byte   | 8 byte    | 4 byte              | N byte       | 

// | prewrite_value length | prewrite_value | ddl_query length | ddl_query | ddl_job_id | crc    |
// | 4 byte                | M byte         | 4 byte           | P byte    | 8 byte     | 4 byte |
func EncodeBinlog(b *Binlog) []byte {
	preWriteKeyLen := len(b.PreWriteKey)
	preWriteValueLen := len(b.PreWriteValue)
	ddlQueryLen := len(b.DDLQuery)
	
	dataSize := 4 + 1 + 8 + 8 + 4 + preWriteKeyLen + 4 + preWriteValueLen + 4 + ddlQueryLen + 8 + 4
	data := make([]byte, dataSize)
	offset := 0

	// magic word
	binary.LittleEndian.PutUint32(data[:4], MagicNumber)
	
	// binlog type
	copy(data[4:5], []byte(fmt.Sprintf("%d", b.Tp)))

	// start ts
	binary.LittleEndian.PutUint64(data[6:14], b.StartTs)

	// commit ts
	binary.LittleEndian.PutUint64(data[14:22], b.CommitTs)

	// prewrite_key length
	binary.LittleEndian.PutUint32(data[22:26], uint32(preWriteKeyLen))

	// prewrite_key
	copy(data[26:26+preWriteKeyLen], b.PreWriteKey)
	offset = 26 + preWriteKeyLen

	// prewrite_value length
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(preWriteValueLen))
	offset += 4

	// prewrite_value
	copy(data[offset:offset+preWriteValueLen], b.PreWriteValue)
	offset += preWriteValueLen

	// ddl_query length
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(ddlQueryLen))
	offset += 4

	// ddl_query
	copy(data[offset:offset+ddlQueryLen], b.DDLQuery)
	offset += ddlQueryLen

	// ddl_job_id
	binary.LittleEndian.PutUint32(data[offset:offset+8], uint32(b.DDLJobID))
	offset += 8

	// crc
	crc := crc32.Checksum(data[4:dataSize-4], crcTable)
	binary.LittleEndian.PutUint32(data[dataSize-4:], crc)

	return data
}

func DecodeBinlog(data []byte) *Binlog {
	b := &Binlog{}
	offset := 0

	magicNum := binary.LittleEndian.Uint32(data[:4])
	log.Print(magicNum)
	if magicNum != MagicNumber {
		log.Fatal("wrong magic number")
	}

	binlogTp := data[4]
	binlogTpInt, err := strconv.Atoi(string(binlogTp))
	if err != nil {
		log.Fatal("transfor to int failed")
	}
	b.Tp = NewBinlogType(binlogTpInt)

	b.StartTs = binary.LittleEndian.Uint64(data[6:14])
	b.CommitTs = binary.LittleEndian.Uint64(data[14:22])

	preWriteKeyLen := binary.LittleEndian.Uint32(data[22:26])
	b.PreWriteKey = data[26:26+preWriteKeyLen]

	offset = 26 + int(preWriteKeyLen)

	preWriteValueLen := binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	b.PreWriteValue = data[offset:offset+int(preWriteValueLen)]
	offset += int(preWriteValueLen)

	ddlQueryLen := binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	b.DDLQuery = data[offset:offset+int(ddlQueryLen)]
	offset += int(ddlQueryLen)

	b.DDLJobID = binary.LittleEndian.Uint64(data[offset:offset+8])

	crc1 := binary.LittleEndian.Uint32(data[len(data)-4:])
	crc2 := crc32.Checksum(data[4:len(data)-4], crcTable)
	if crc1 != crc2 {
		log.Fatalf("wrong crc, crc1: %d, crc2: %d", crc1, crc2)
	}

	return b
}