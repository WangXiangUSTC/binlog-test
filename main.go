package main

import (
	//"fmt"
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"time"
	//"encoding/gob"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"

	"github.com/pingcap/binlog/binlog"
	"github.com/pingcap/binlog/binlog-pb"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}

	var dataSize, dataSizeCompress int
	var EncodeBinlogTime, DecodeBinlogTime, CompressTime, UnCompressTime time.Duration
	var GenerateBinlogFn func(int) ([]byte, time.Duration)
	var DecodeBinlogFn func([]byte)
	if cfg.Mode == "pb" {
		GenerateBinlogFn = GenerateBinlogPb
		DecodeBinlogFn = DecodeBinlogPb
	} else {
		GenerateBinlogFn = GenerateBinlog
		DecodeBinlogFn = DecodeBinlog
	}

	for i := 0; i < cfg.Count; i++ {
		binlog, encodeT := GenerateBinlogFn(cfg.Size)

		if cfg.Compress == "Y" {
			t5 := time.Now()
			b, err := compress(binlog, cfg.Method)
			if err != nil {
				log.Fatal(err)
			}

			t6 := time.Now()

			binlog, err = unCompress(b, cfg.Method)
			if err != nil {
				log.Fatal(err)
			}

			t7 := time.Now()

			CompressTime += t6.Sub(t5)
			UnCompressTime += t7.Sub(t6)
			dataSizeCompress += len(b)
		}
		dataSize += len(binlog)

		t3 := time.Now()
		DecodeBinlogFn(binlog)
		t4 := time.Now()

		EncodeBinlogTime += encodeT
		DecodeBinlogTime += t4.Sub(t3)
	}

	log.Printf("encode binlog: %v, decode binlog: %v, data size: %d, compress data size: %d, CompressTime: %v, UnCompressTime: %v \n",
		cfg.Method, EncodeBinlogTime, DecodeBinlogTime, dataSize, dataSizeCompress, CompressTime, UnCompressTime)
}

func GenerateBinlogPb(size int) ([]byte, time.Duration) {
	/*
		sequence := make([]binlog_pb.MutationType, 0, 10)
		for i := 0; i < 10; i++ {
			sequence = append(sequence, binlog_pb.MutationType_Insert)
		}
		mutation := binlog_pb.TableMutation {
			TableId: 10,
			InsertedRows: GenerateRows(size),
			Sequence: sequence,
		}

		preWriteValue := &binlog_pb.PrewriteValue {
			SchemaVersion: 100000,
			Mutations: []binlog_pb.TableMutation{mutation},
		}

		p, err := preWriteValue.Marshal()
		if err != nil {
			log.Fatal(err)
		}
	*/

	b := &binlog_pb.Binlog{
		Tp:            binlog_pb.BinlogType_Prewrite,
		StartTs:       randInt64(10, 9999999),
		CommitTs:      randInt64(10, 9999999),
		DdlJobId:      randInt64(1, 10000),
		PrewriteKey:   []byte(randString(1000)),
		PrewriteValue: []byte(randString(size)),
		DdlQuery:      []byte(randString(100)),
	}

	start := time.Now()
	data, err := b.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	return data, time.Since(start)
}

func DecodeBinlogPb(data []byte) {
	b := &binlog_pb.Binlog{}
	err := b.Unmarshal(data)
	if err != nil {
		log.Fatal(err)
	}
	/*
		p := &binlog_pb.PrewriteValue{}
		err = p.Unmarshal(b.PrewriteValue)
		if err != nil {
			log.Fatal(err)
		}
	*/
}

func GenerateBinlog(size int) ([]byte, time.Duration) {
	/*
		sequence := make([]binlog.MutationType, 0, 10)
		for i := 0; i < 10; i++ {
			sequence = append(sequence, binlog.Insert)
		}
		mutation := &binlog.TableMutation {
			TableID: 10,
			InsertedRows: GenerateRows(size),
			Sequence: sequence,
		}

		preWriteValue := &binlog.PreWriteValue {
			SchemaVersion: 100000,
			Mutations: []*binlog.TableMutation{mutation},
		}

		var p bytes.Buffer
		enc := gob.NewEncoder(&p)
		err := enc.Encode(&preWriteValue)
		if err != nil {
			log.Fatal("encode:", err)
		}
	*/

	b := &binlog.Binlog{
		Tp:            binlog.PreWrite,
		StartTs:       uint64(randInt64(10, 9999999)),
		CommitTs:      uint64(randInt64(10, 9999999)),
		DDLJobID:      uint64(randInt64(1, 10000)),
		PreWriteKey:   []byte(randString(1000)),
		PreWriteValue: []byte(randString(size)),
		DDLQuery:      []byte(randString(100)),
	}

	start := time.Now()
	return binlog.EncodeBinlog(b), time.Since(start)
}

func DecodeBinlog(data []byte) {
	_ = binlog.DecodeBinlog(data)
}

func GenerateRows(size int) [][]byte {
	result := make([][]byte, 0, 10)
	for i := 0; i < 10; i++ {
		tmp := []byte(randString(size))
		result = append(result, tmp)
	}
	return result
}

func compress(in []byte, method string) ([]byte, error) {
	var (
		buffer bytes.Buffer
		out    []byte
		err    error
	)
	switch method {
	case "gzip":
		writer := gzip.NewWriter(&buffer)
		_, err = writer.Write(in)
		if err != nil {
			writer.Close()
			return out, err
		}
		err = writer.Close()
		if err != nil {
			return out, err
		}
	case "flate":
		writer, err := flate.NewWriter(&buffer, 1)
		if err != nil {
			//writer.Close()
			return out, err
		}
		_, err = writer.Write(in)
		if err != nil {
			writer.Close()
			return out, err
		}
		err = writer.Close()
		if err != nil {
			return out, err
		}
	case "zlib":
		writer := zlib.NewWriter(&buffer)
		_, err = writer.Write(in)
		if err != nil {
			writer.Close()
			return out, err
		}
		err = writer.Close()
		if err != nil {
			return out, err
		}
	}

	return buffer.Bytes(), nil
}

func unCompress(in []byte, method string) ([]byte, error) {
	switch method {
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(in))
		if err != nil {
			var out []byte
			return out, err
		}
		defer reader.Close()
		return ioutil.ReadAll(reader)
	case "flate":
		reader := flate.NewReader(bytes.NewReader(in))
		defer reader.Close()
		return ioutil.ReadAll(reader)
	case "zlib":
		reader, err := zlib.NewReader(bytes.NewReader(in))
		if err != nil {
			var out []byte
			return out, err
		}
		defer reader.Close()
		return ioutil.ReadAll(reader)
	}

	return nil, nil
}
