package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Try to JSON decode the bytes
func tryUnmarshal(b []byte) error {
	var v interface{}
	err := json.Unmarshal(b, &v)
	return err
}

//Copy JSON Rows and return list of errors
func copyJSONRows(i *Import, reader *bufio.Reader, ignoreErrors bool) (error, int, int) {
	success := 0
	failed := 0

	for {
		// ReadBytes instead of a Scanner because it can deal with very long lines
		// which happens often with big JSON objects
		line, err := reader.ReadBytes('\n')

		if err == io.EOF {
			err = nil
			break
		}

		if err != nil {
			err = fmt.Errorf("%s: %s", err, line)
			return err, success, failed
		}

		err = tryUnmarshal(line)
		if err != nil {
			failed++
			if ignoreErrors {
				os.Stderr.WriteString(string(line))
				continue
			} else {
				err = fmt.Errorf("%s: %s", err, line)
				return err, success, failed
			}
		}

		err = i.AddRow(string(line))
		if err != nil {
			failed++
			if ignoreErrors {
				os.Stderr.WriteString(string(line))
				continue
			} else {
				err = fmt.Errorf("%s: %s", err, line)
				return err, success, failed
			}
		}

		success++
	}

	return nil, success, failed
}

func get_type(v interface{}) string {
	s, _ := v.(string)
	i, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		if i < 2147483648 {
			return "integer"
		} else {
			return "bigint"
		}
	}
	if err != nil {
		_, err := strconv.ParseFloat(s, 32)
		if err == nil {
			return "double precision"
		}
	}
	return "character varying"
}

func check_types(m *map[string]interface{}, dbtypes *map[string]string, db *sql.DB, tb string) error {
	for k, v := range *m {
		dt := get_type(v)
		if v2, ok := (*dbtypes)[k]; ok {
			if v2 != dt {
				if dt == "double precision" ||
					dt == "bigint" {
					db.Query(fmt.Sprintf("alter table %s alter column \"%s\" type %s", tb, k, dt))
				}
			}
		} else {
			_, err := db.Query(fmt.Sprintf("alter table %s add column \"%s\" %s", tb, k, dt))
			if err != nil {
				return err
			}
		}
		(*dbtypes)[k] = dt
	}
	return nil
}

func chan_work(idx int, connStr string, schema string, tb string, q <-chan []byte,
	wg *sync.WaitGroup, mx *sync.Mutex, ctb chan int) error {
	defer wg.Done()

	db, err := connect(connStr, schema)
	if err != nil {
		return err
	}
	defer db.Close()

	var stmt *sql.Stmt
	var b_create_table bool = false
	var keys []string
	var params []interface{}
	var dbtypes map[string]string = make(map[string]string)

	handle_line := func(line []byte, db *sql.DB, tableName string) error {
		if b_create_table == false {
			if idx == 0 {
				defer close(ctb)
				_, err := db.Query(fmt.Sprintf("drop table if exists %s; create table %s ()", tableName, tableName))
				if err != nil {
					return err
				}
			} else {
				<-ctb
			}
			b_create_table = true
		}
		var o interface{}
		err := json.Unmarshal(line, &o)
		if err != nil {
			return err
		}

		m := o.(map[string]interface{})

		if stmt == nil {
			mx.Lock()
			defer mx.Unlock()
			check_types(&m, &dbtypes, db, tableName)
			keys = make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			params = make([]interface{}, len(keys))
			key := strings.Join(keys, "\",\"")
			phs := make([]string, len(keys))
			for i := range keys {
				phs[i] = fmt.Sprintf("$%d", i+1)
			}
			sql := fmt.Sprintf("insert into %s(\"%s\")values(%s)",
				tableName, key, strings.Join(phs, ","))
			stmt, err = db.Prepare(sql)
			if err != nil {
				return err
			}
		}

		m2 := make(map[string]struct{}) // check added column
		for k := range m {
			m2[k] = struct{}{}
		}
		for i, v := range keys {
			params[i] = m[v]
			delete(m2, v)
		}
		if len(m2) > 0 {
			return errors.New("new col")
		}
		_, err = stmt.Exec(params...)
		if err != nil {
			log.Println(err)
			return err
		}
		return nil
	}

	for line := range q {
		err = handle_line(line, db, tb)
		if err != nil {
			stmt = nil
			err = handle_line(line, db, tb)
			if err != nil {
				log.Println(string(line))
				log.Println(err)
				return err
			}
		}
	}

	return nil
}
func importJSON(filename string, connStr string, schema string, tableName string, ignoreErrors bool, dataType string, threadn int) error {

	var reader *bufio.Reader
	q := make(chan []byte, 100000)

	if filename == "" {
		reader = bufio.NewReader(os.Stdin)
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		reader = bufio.NewReader(file)
	}
	start_time := time.Now().UnixNano() / 1000000

	rown_ch := make(chan int, 10)

	reads := func() error {
		defer close(q)
		linen := 0
		for {
			line, _, err := reader.ReadLine()
			linen++
			if io.EOF == err {
				fmt.Println(fmt.Sprintf("read %d rows in %d ms", linen, time.Now().UnixNano()/1000000-start_time))
				rown_ch <- linen
				break
			}
			if err != nil {
				return err
			}
			line2 := make([]byte, len(line))
			copy(line2, line)
			q <- line2
		}

		for {
			l := len(q)
			if l == 0 {
				break
			}
			fmt.Println(l)
			time.Sleep(time.Second)
		}
		return nil
	}
	var wg sync.WaitGroup
	var mux sync.Mutex
	go reads()
	creat_tbc := make(chan int)
	for i := 0; i < threadn; i++ {
		wg.Add(1)
		go chan_work(i, connStr, schema, tableName, q, &wg, &mux, creat_tbc)
	}

	wg.Wait()

	rown := <-rown_ch
	use_t := time.Now().UnixNano()/1000000 - start_time
	fmt.Println(fmt.Sprintf("imported into %s.%s, %d row, use %d ms, %d/s", schema, tableName,
		rown, use_t, rown*1000/int(use_t)))

	return nil
}
