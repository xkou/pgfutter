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
					db.Query(fmt.Sprintf("alter table %s alter column %s type %s", tb, k, dt))
				}
			}
		} else {
			_, err := db.Query(fmt.Sprintf("alter table %s add column %s %s", tb, k, dt))
			if err != nil {
				return err
			}
		}
		(*dbtypes)[k] = dt
	}
	return nil
}

var stmt *sql.Stmt
var b_create_table bool = false
var keys []string
var params []interface{}
var dbtypes map[string]string = make(map[string]string)

func handle_line(line []byte, db *sql.DB, tableName string) error {
	if b_create_table == false {
		_, err := db.Query(fmt.Sprintf("drop table if exists %s; create table %s ()", tableName, tableName))
		if err != nil {
			return err
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
		return err
	}
	return nil
}

func importJSON(filename string, connStr string, schema string, tableName string, ignoreErrors bool, dataType string) error {

	db, err := connect(connStr, schema)
	if err != nil {
		return err
	}
	defer db.Close()

	lineNumber := 0
	if filename == "" {
		//	reader := bufio.NewReader(os.Stdin)
		//	err, success, failed = copyJSONRows(i, reader, ignoreErrors)
	} else {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()

		bar := NewProgressBar(file)
		reader := bufio.NewReader(io.TeeReader(file, bar))
		bar.Start()

		for {
			line, _, err := reader.ReadLine()
			lineNumber += 1
			if io.EOF == err {
				break
			}
			if err != nil {
				return err
			}
			err = handle_line(line, db, tableName)
			if err != nil {
				stmt = nil
				err = handle_line(line, db, tableName)
				if err != nil {
					log.Println("err lineno:", lineNumber)
					return err
				}
			}
		}
		bar.Finish()
	}

	fmt.Println(fmt.Sprintf("%d rows imported into %s.%s", lineNumber, schema, tableName))

	return nil
}
