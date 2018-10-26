package ydb

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/boylee1111/ydb/ydbserverrpc"
	"go.etcd.io/bbolt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type tableMeta struct {
	tableName       string    // Table name
	columnsFamilies []string  // Column family
	memTableLimit   int       // Max limit rows for table in memory
	creationTime    time.Time // Table create time
}

type ydbTable struct {
	metadata   tableMeta
	data       map[string]ydbColumn // Row Key -> column data
	dataLocker *sync.RWMutex        // Mutex for data store
	//inOpen     bool                 // Is opened
}

type ydbColumn struct {
	columns map[string]string // Key is column family:qualifier, val is value
}

func (col* ydbColumn) merge(other ydbColumn) {
	for key, value := range other.columns {
		col.columns[key] = value
	}
}

func (col* ydbColumn) toString() string {
	ret, _ := json.Marshal(col)
	return string(ret)
}

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func (table* ydbTable) filePath() string {
	return "." + table.metadata.tableName + "_db"
}


func (table* ydbTable) flush(ydb* ydbServer) error {
	f, err := os.OpenFile(table.filePath(), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
		return err
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
		return err
	}
	countLine, err := lineCounter(bufio.NewReader(f))
	if err != nil {
		log.Fatal(err)
		return err
	}
	f.Close();
	db := ydb.indexDB
	f, err = os.OpenFile(table.filePath(), os.O_APPEND, 0755)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	w := bufio.NewWriter(f)
	// Here we append the memTable into the file:
	// RowKey, {"a:b": "1", "c:d": "2"}
	for key, value := range table.data {
		db.Batch(func (tx* bbolt.Tx) error {
			b := tx.Bucket([]byte(table.metadata.tableName))
			v := b.Get([]byte(key))
			lines := make([]int, 0)
			if v != nil {
				json.Unmarshal(v, &lines)
			}
			lines = append(lines, countLine)
			v,err = json.Marshal(lines)
			if err != nil {
				log.Fatal(err)
				return err
			}
			b.Put([]byte(key), v)
			return nil
		})
		line := key + "," + value.toString()
		fmt.Fprintln(w, line)
	}
	return nil;
}


func (table* ydbTable) PutRow(ydb* ydbServer,rowKey string, updated map[string]string) error {
	for key, value := range updated {
		table.data[rowKey].columns[key] = value
	}
	if len(table.data) > table.metadata.memTableLimit {
		table.flush(ydb)
	}
	return nil
}

func (table *ydbTable) GetRow(ydb* ydbServer,rowKey string) string {
	fmt.Println("Get Row")
	// TODO: get record
	col, ext := table.data[rowKey]
	if ext == false {
		return ""
	}
	// Get column from file line by line, merge them
	db := ydb.indexDB
	f, err := os.Open(table.filePath())
	if err != nil {
		log.Fatal(err)
		return ""
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	err = db.View(func(tx *bbolt.Tx) error{
		b := tx.Bucket([]byte(table.metadata.tableName))
		v := b.Get([]byte(rowKey))
		lines := make([]int, 0)
		sort.Ints(lines)
		json.Unmarshal(v, &lines)
		cnt := 0
		for l := range lines {
			for cnt != l{
				reader.ReadString(byte('\n'))
				cnt += 1
			}
			line, err := reader.ReadString(byte('\n'))
			if err != nil {
				return err
			}
			parts := strings.Split(line, ",")
			var anotherCol ydbColumn
			json.Unmarshal([]byte(parts[1]), &anotherCol)
			col.merge(anotherCol)
			cnt += 1
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
		return ""
	}
	ret, err := json.Marshal(col.columns)
	if err != nil {
		log.Fatal(err)
		return ""
	}
	return string(ret)
}

func (table *ydbTable) GetRows(args *ydbserverrpc.GetRowsArgs, reply *ydbserverrpc.GetRowsReply) error {
	fmt.Println("Get Rows")
	// TODO: get records
	return nil
}

func (table *ydbTable) GetColumnByRow(args *ydbserverrpc.GetColumnByRowArgs, reply *ydbserverrpc.GetColumnByRowReply) error {
	fmt.Println("Get Column By Row")
	// TODO: get records
	return nil
}

func (table *ydbTable) MemTableLimit(args *ydbserverrpc.MemTableLimitArgs, reply *ydbserverrpc.MemTableLimitReply) error {
	fmt.Println("Mem Table Limit")
	// TODO: update mem limit, check mem size
	return nil
}
