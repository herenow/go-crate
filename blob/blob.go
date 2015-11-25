package blob

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	_ "github.com/herenow/go-crate"
	"io"
	"net/http"
	"time"
)

type Driver struct {
	url string
	db  *sql.DB
}

type Table struct {
	Name string
	drv  *Driver
	c    *http.Client
}

func New(crate_url string) (*Driver, error) {
	db, err := sql.Open("crate", crate_url)
	if err != nil {
		return nil, err
	}
	return &Driver{
		url: crate_url,
		db:  db,
	}, nil
}

// NewTable create new blob table with name and extra int to specify shards and replicas
func (d *Driver) NewTable(name string, shards ...int) (*Table, error) {
	sql := fmt.Sprintf(
		"create blob table %s",
		name,
	)
	if len(shards) == 1 {
		sql = fmt.Sprintf(
			"create blob table %s clustered into %d shards",
			name, shards[0],
		)
	}
	if len(shards) >= 2 {
		sql = fmt.Sprintf(
			"create blob table %s clustered into %d shards with (number_of_replicas=%d)",
			name, shards[0], shards[1],
		)
	}
	_, err := d.db.Exec(sql)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name: name,
		drv:  d,
		c:    new(http.Client),
	}, nil
}

func (d *Driver) GetTable(name string) (*Table, error) {
	_, err := d.db.Exec(
		"select count(*) from information_schema.tables where table_name = '?' and schema_name = 'blob'",
		name,
	)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name: name,
		drv:  d,
		c:    new(http.Client),
	}, nil
}

type Entry struct {
	Digest       string
	LastModified time.Time
}

func Sha1Digest(r io.Reader) string {
	h := sha1.New()
	io.Copy(h, r)
	return hex.EncodeToString(h.Sum(nil))
}

func (t *Table) Upload(digest string, r io.Reader) (*Entry, error) {
	url := fmt.Sprintf("%s/_blobs/%s/%s", t.drv.url, t.Name, digest)
	req, err := http.NewRequest("PUT", url, r)
	if err != nil {
		return nil, err
	}
	_, err = t.c.Do(req)
	if err != nil {
		return nil, err
	}
	return &Entry{
		Digest: digest,
	}, nil
}
