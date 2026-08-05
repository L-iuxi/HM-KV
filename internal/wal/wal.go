package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrCorrupt  = errors.New("wal: log file is corrupt")
	ErrNotFound = errors.New("wal: file not found")
	ErrClosed   = errors.New("wal: log is closed")
)

type RecType byte

const (
	RecTypeEntry    RecType = 1 //存储日志
	RecTypeState    RecType = 2 //存储raft信息
	RecTypeSnapshot RecType = 3 //快照
)
const (
	recordHeaderSize = 4 + 1 + 4
	//NodeId + LogType + CRC
)

type Wal struct {
	file   *os.File      //数据文件
	dir    string        //数据目录
	writer *bufio.Writer //缓冲写，先放入内存缓冲区，再一次刷盘
	seq    uint64        //文件序号
}

type LogHeader struct {
	RecType RecType //数据类型
	Len     int32   //数据长度
	CRC     uint32  //CRC校验
}

type WalEntry struct {
	Header LogHeader //数据信息
	Data   []byte    //数据
}

func (w *Wal) Dir() string {
	return w.dir
}

// 创建目录下的WAL文件，path 为数据目录。
func NewWal(dir string) *Wal {
	if err := os.MkdirAll(dir, 0755); err != nil {
		panic(err)
	}

	path := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	writer := bufio.NewWriter(file)

	return &Wal{
		file:   file,
		writer: writer,
		dir:    dir,
		seq:    0,
	}
}

// 写WAL
func (w *Wal) Write(rectype RecType, data []byte) error {
	if w.writer == nil {
		return ErrClosed
	}

	//创建header，写header信息
	header := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(1+len(data)+4))
	header[4] = byte(rectype)

	//算crc
	crc := crc32.NewIEEE()
	crc.Write([]byte{byte(rectype)})
	crc.Write(data)
	binary.BigEndian.PutUint32(header[5:9], crc.Sum32())

	//写入磁盘
	if _, err := w.writer.Write(header); err != nil {
		return err
	}

	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	return w.writer.Flush()
}

// 从WAL文件里面加载所有日志
func (w *Wal) LoadAll() (records [][]byte, types []RecType, err error) {
	// 获取WAL文件列表
	names, err := listLogFiles(w.dir)
	if err != nil {
		return nil, nil, err
	}

	// 读取文件内容
	for _, name := range names {
		path := filepath.Join(w.dir, name)
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, err
		}
		defer f.Close()

		//把读取的数据先放到缓冲区，减少系统调用
		reader := bufio.NewReader(f)
		for {
			// 获取header
			header := make([]byte, recordHeaderSize)
			_, err := io.ReadFull(reader, header)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, err
			}

			// 解码头
			length := binary.BigEndian.Uint32(header[0:4])
			recType := RecType(header[4])
			expectedCrc := binary.BigEndian.Uint32(header[5:9])

			// 获取数据
			dataLen := length - (1 + 4)
			data := make([]byte, dataLen)
			if _, err := io.ReadFull(reader, data); err != nil {
				return nil, nil, err
			}

			//校验crc
			crc := crc32.NewIEEE()
			crc.Write([]byte{byte(recType)})
			crc.Write(data)
			if crc.Sum32() != expectedCrc {
				return nil, nil, ErrCorrupt
			}

			// 加入数据
			records = append(records, data)
			types = append(types, recType)
		}
	}

	return records, types, nil
}

// 列出所有WAL文件
func listLogFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Filter for .log files
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".log") {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}

// 删除不需要的旧日志文件
func (w *Wal) Truncate(lastIndex uint64) error {
	names, err := listLogFiles(w.dir)
	if err != nil {
		return err
	}

	var seq uint64
	for _, name := range names {
		fmt.Sscanf(name, "%x.wal", &seq)
		if seq <= lastIndex {
			if err := os.Remove(filepath.Join(w.dir, name)); err != nil {
				// Log error but continue trying to delete others
			}
		}
	}
	return nil
}
func (w *Wal) Exists() bool {
	_, err := os.Stat(filepath.Join(w.dir, "wal.log"))
	return err == nil
}
