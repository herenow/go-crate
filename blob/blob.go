package blob

import (
	"database/sql"
	_ "github.com/herenow/go-crate"
)

type Driver struct {
	url string
	db  *sql.DB
}

type Table struct {
	Name string
	drv  *Driver
}

func New(crate_url string) (*Driver, error) {
	db, err := sql.Open("crate", crate_url)
	if err != nil {
		return nil, err
	}
	return &Driver{
		url: crate_url,
		db:  db,
	}
}

func (d *Driver) NewTable(name string, shards, replicas int) (*Table, error) {
	_, err := d.db.Exec(
		"create blob table ? clustered into ? shards with (number_of_replicas=?)",
		name, shards, replicas,
	)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name: name,
		drv:  d,
	}
}

func (d *Driver) GetTable(name string) (*Table, error) {
	r, err := d.db.Exec(
		"select count(*) from information_schema.tables where table_name = '?' and schema_name = 'blob';",
		name,
	)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name: name,
		drv:  d,
	}, nil
}
