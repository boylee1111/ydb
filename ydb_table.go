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

type TableMeta struct {
	TableName       string    // Table name
	ColumnsFamilies []string  // Column family
	MemTableLimit   int       // Max limit rows for table in memory
	CreationTime    time.Time // Table create time
}

type ydbTable struct {
	metadata   TableMeta
	data       map[string]YDBColumn // Row Key -> column data
	dataLocker *sync.RWMutex        // Mutex for data store
	//inOpen     bool                 // Is opened
}

type YDBColumn struct {
	Columns map[string]string // Key is column family:qualifier, val is value
}

func (col *YDBColumn) merge(other YDBColumn) {
	for key, value := range other.Columns {
		col.Columns[key] = value
	}
}

func (col *YDBColumn) toString() string {
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

func (table *ydbTable) filePath() string {
	return "./" + table.metadata.TableName + ".ydb"
}

func (table *ydbTable) flush(ydb *ydbServer) error {
	f, err := os.OpenFile(table.filePath(), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Println("Open file error")
		log.Fatal(err)
		return err
	}
	//if err := f.Close(); err != nil {
	//	log.Fatal(err)
	//	return err
	//}
	countLine, err := lineCounter(bufio.NewReader(f))
	f.Close()

	db := ydb.indexDB
	f, err = os.OpenFile(table.filePath(), os.O_APPEND|os.O_WRONLY, 0755)
	defer f.Close()
	if err != nil {
		fmt.Println("Open file error")
		log.Fatal(err)
	}
	//if err := f.Close(); err != nil {
	//	log.Fatal(err)
	//}
	//w := bufio.NewWriter(f)
	// Here we append the memTable into the file:
	// RowKey, {"a:b": "1", "c:d": "2"}
	for key, value := range table.data {
		db.Batch(func(tx *bbolt.Tx) error {
			b := tx.Bucket([]byte(table.metadata.TableName))
			if b == nil {
				b, err = tx.CreateBucket([]byte(table.metadata.TableName))
			}
			v := b.Get([]byte(key))
			lines := make([]int, 0)
			if v != nil {
				json.Unmarshal(v, &lines)
			}
			lines = append(lines, countLine)
			v, err = json.Marshal(lines)
			if err != nil {
				fmt.Println("Marshal error")
				log.Fatal(err)
				return err
			}
			b.Put([]byte(key), v)
			return nil
		})
		line := key + "|" + value.toString()
		if _, err := f.WriteString(line + "\n"); err != nil {
			panic(err)
		}
		countLine ++
		//fmt.Fprintln(w, line)
	}

	table.data = make(map[string]YDBColumn)
	f.Sync()
	return nil
}

func (table *ydbTable) PutRow(ydb *ydbServer, rowKey string, updated map[string]string) error {
	if _, ok := table.data[rowKey]; !ok {
		table.data[rowKey] = YDBColumn{
			Columns: make(map[string]string),
		}
	}
	for key, value := range updated {
		table.data[rowKey].Columns[key] = value
	}
	fmt.Println("update content " + rowKey)
	if len(table.data) > table.metadata.MemTableLimit {
		fmt.Println("Before flush")
		table.flush(ydb)
		fmt.Println("After flush")
	}
	return nil
}

func (table *ydbTable) GetRow(ydb *ydbServer, rowKey string) string {
	fmt.Println("Get Row " + rowKey)
	// TODO: get record
	col, ok := table.data[rowKey]
	if !ok {
		col = YDBColumn{
			Columns: make(map[string]string),
		}
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
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(table.metadata.TableName))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(rowKey))
		lines := make([]int, 0)
		json.Unmarshal(v, &lines)
		sort.Ints(lines)
		cnt := 0
		for _, l := range lines {
			for cnt != l {
				reader.ReadString(byte('\n'))
				cnt += 1
			}
			line, err := reader.ReadString(byte('\n'))
			if err != nil {
				return err
			}
			parts := strings.Split(line, "|")
			var anotherCol YDBColumn
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
	ret, err := json.Marshal(col.Columns)
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
