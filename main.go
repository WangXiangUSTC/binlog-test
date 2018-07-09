package main

import (
	"fmt"
	"log"
	"os"
	"time"
	"bytes"
	"io/ioutil"
	"encoding/gob"
	"compress/gzip"
	//"compress/bzip2"
	"compress/flate"
	//"compress/lzw"
	"compress/zlib"

	//"github.com/ngaut/log"
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
	var GenerateBinlogTime, DecodeBinlogTime, CompressTime, UnCompressTime, AllTime time.Duration
	var t1, t2, t3, t4 time.Time
	var GenerateBinlogFn func(int) []byte
	var DecodeBinlogFn func([]byte)
	if cfg.Mode == "pb" {
		GenerateBinlogFn = GenerateBinlogPb
		DecodeBinlogFn = DecodeBinlogPb
	} else {
		GenerateBinlogFn = GenerateBinlog
		DecodeBinlogFn = DecodeBinlog
	}

	for i := 0; i < cfg.Count; i++ {
		t1 = time.Now()
		binlog := GenerateBinlogFn(cfg.Size)
		t2 = time.Now()
		//fmt.Println(binlog)

		if cfg.Compress == "Y" {
			t5 := time.Now()
			b, err := encode(binlog, cfg.Method)
			if err != nil {
				log.Fatal(err)
			}

			t6 := time.Now()

			binlog, err = decode(b, cfg.Method)
			if err != nil {
				log.Fatal(err)
			}

			t7 := time.Now()

			CompressTime += t6.Sub(t5)
			UnCompressTime += t7.Sub(t6)
			dataSizeCompress += len(b)
		}
		dataSize += len(binlog)
		//log.Printf("before compress %d, after %d", len(binlog), len(b))
		//log.Printf("%v", buf)
	
		t3 = time.Now()

		DecodeBinlogFn(binlog)
		t4 = time.Now()
	}

	GenerateBinlogTime += t2.Sub(t1)
	DecodeBinlogTime += t4.Sub(t3)
	AllTime += t4.Sub(t1)

	log.Printf("method :%s, generate binlog: %v, decode binlog: %v, all: %v, data size: %d, compress data size: %d, encode: %v, decode: %v \n", 
		cfg.Method, GenerateBinlogTime, DecodeBinlogTime, AllTime, dataSize, dataSizeCompress, CompressTime, UnCompressTime)
}

func GenerateBinlogPb(size int) []byte {
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

	b := &binlog_pb.Binlog {
		Tp: binlog_pb.BinlogType_Prewrite,
		StartTs:  randInt64(10, 9999999),
		CommitTs: randInt64(10, 9999999),
		DdlJobId: randInt64(1, 10000),
		PrewriteKey: []byte(randString(1000)),
		PrewriteValue: p,
		DdlQuery:    []byte(randString(100)),
	}
	
	data, err := b.Marshal()
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func DecodeBinlogPb(data []byte) {
	b := &binlog_pb.Binlog{}
	err := b.Unmarshal(data)
	if err != nil {
		log.Fatal(err)
	}
	p := &binlog_pb.PrewriteValue{}
	err = p.Unmarshal(b.PrewriteValue)
	if err != nil {
		log.Fatal(err)
	}


	//log.Printf("schema version: %d", p.GetSchemaVersion())
}

func GenerateBinlog(size int) []byte {
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

	/*
	p, err := json.Marshal(preWriteValue)
	if err != nil {
		log.Fatal(err)
	}
	*/

	b := &binlog.Binlog {
		Tp: binlog.PreWrite,
		StartTs:  uint64(randInt64(10, 9999999)),
		CommitTs: uint64(randInt64(10, 9999999)),
		DDLJobID: uint64(randInt64(1, 10000)),
		PreWriteKey: []byte(randString(1000)),
		PreWriteValue: p.Bytes(),
		DDLQuery:    []byte(randString(100)),
	}

	d := binlog.EncodeBinlog(b)
	_ = binlog.DecodeBinlog(d)

	var data bytes.Buffer
	enc = gob.NewEncoder(&data)
	err = enc.Encode(&b)
	if err != nil {
		log.Fatal("encode:", err)
	}
	/*
	data, err := json.Marshal(b)
	if err != nil {
		log.Fatal(err)
	}
	*/

	return data.Bytes()
}

func DecodeBinlog(data []byte) {
	b := &binlog.Binlog{}

	//var buf bytes.Buffer
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	err := dec.Decode(&b)
	if err != nil {
		log.Fatal("decode:", err)
	}
	/*
	err := json.Unmarshal(data, b)
	if err != nil {
		log.Fatal(err)
	}
	*/

	p := &binlog.PreWriteValue{}

	dec = gob.NewDecoder(bytes.NewBuffer(b.PreWriteValue))
	err = dec.Decode(&p)
	if err != nil {
		log.Fatal("decode:", err)
	}
	/*
	err = json.Unmarshal(b.PreWriteValue, p)
	if err != nil {
		log.Fatal(err)
	}
	*/

	//log.Printf("schema version: %d", p.SchemaVersion)
}

func GenerateRows(size int) [][]byte {
	result := make([][]byte, 0, 10)
	for i := 0; i < 10; i++ {
		tmp := []byte(fmt.Sprintf("abcdefghijklmn%s", randString(size)))
		result = append(result, tmp)	
	}
	return result
}

func encode(in []byte, method string) ([]byte, error) {
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

func decode(in []byte, method string) ([]byte, error) {
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
