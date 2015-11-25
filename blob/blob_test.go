package blob

import (
	"strings"
	"testing"
)

var (
	driver *Driver
)

func init() {
	var err error
	driver, err = New("http://localhost:4200")
	if err != nil {
		panic(err)
	}
}

func TestGetTable(t *testing.T) {
	table, err := driver.GetTable("myblobs")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(table.Name)
}

func TestNewTable(t *testing.T) {
	table, err := driver.NewTable("myblobs123", 3, 2)
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(table.Name)
}

func TestUpload(t *testing.T) {
	data := "asdadfadfasdfasdfafdast"
	r := strings.NewReader(data)
	digest := Sha1Digest(r)
	r.Seek(0, 0)
	table, err := driver.GetTable("myblobs")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}
	t.Log(table.Name)
	_, err = table.Upload(digest, r)
	if err != nil {
		t.Error(err.Error())
	}
}
