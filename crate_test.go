package crate

import "testing"
import "database/sql"
import "log"

func connect() (*sql.DB, error) {
	return sql.Open("crate", "http://127.0.0.1:4200/")
}

func TestConnect(t *testing.T) {
	_, err := connect()

	if err != nil {
		t.Fatalf("Error connecting: %s", err.Error())
	}
}

func TestQuery(t *testing.T) {
	db, _ := connect()

	rows, err := db.Query("select count(*) from sys.cluster limit ?", 1)

	if err != nil {
		t.Fatalf("Error on db.Query: %s", err.Error())
	}

	cols, _ := rows.Columns()
	n := len(cols)

	if n != 1 {
		t.Error(
			"rows.Columns expected 1, but got,",
			n,
			cols,
		)
	}

	rows, err = db.Query("select column_name from information_schema.columns")

	for rows.Next() {
		var column string

		if err = rows.Scan(&column); err != nil {
                    t.Error(err)
		}
	}
}

func TestExec(t *testing.T) {
	db, _ := connect()

	result, err := db.Exec("create table go_crate (id int, str string)")

	if err != nil {
		t.Error(err)
	}

	result, err = db.Exec("drop table go_crate")

	if err != nil {
		t.Error(err)
	}
}
