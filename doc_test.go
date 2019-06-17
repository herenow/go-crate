package crate_test

import (
	"database/sql"
	"fmt"
	_ "github.com/herenow/go-crate"
	"testing"
)

func TestExampleCrateDrive_Open(t *testing.T) {
	_, err := sql.Open("crate", "http://localhost:4200")

	if err != nil {
		t.Fatal(err)
	}
}

func TestExampleCrateDrive_OpenUsernamePassword(t *testing.T) {
	_, err := sql.Open("crate", "http://username:password@localhost:4200")

	if err != nil {
		t.Fatal(err)
	}
}

func TestExampleCrateDriver_Query(t *testing.T) {
	db, err := sql.Open("crate", "http://localhost:4200")

	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT name FROM sys.cluster")

	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("%s\n", name)
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
