package crate

import (
	"database/sql/driver"
	"database/sql"
	"net/http"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"bytes"
	"errors"
	"io"
	"net/url"
)

// Crate conn structure
type CrateDriver struct {
	Url string // Crate http endpoint url
}

// Init a new "Connection" to a Crate Data Storage instance.
// Note that the connection is not tested until the first query.
// crate_url example: http://localhost:4200/
func (c *CrateDriver) Open(crate_url string) (driver.Conn, error) {
	u, err := url.Parse(crate_url)

	if err != nil {
		return nil, err
	}

	sanUrl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	c.Url= sanUrl

	return c, nil
}

// Json response struct
type endpointResponse struct {
	Error struct {
		Message string
		Code int
	} `json:"error"`
	Cols []string `json:"cols"`
	Duration int `json:"duration"`
	Rowcount int64  `json:"rowcount"`
	Rows [][]interface{} `json:"rows"`
}

// Crate json query struct
type endpointQuery struct {
	Stmt string `json:"stmt"`
	Args []driver.Value `json:"args,omitempty"`
}

// Query the database using prepared statements.
// Read: https://crate.io/docs/stable/sql/rest.html for more information about the returned response.
// Example: crate.Query("SELECT * FROM sys.cluster LIMIT ?", 10)
// "Parameter Substitution" is also supported, read, https://crate.io/docs/stable/sql/rest.html#parameter-substitution
// This is the internal querie function
func (c *CrateDriver) query(stmt string, args []driver.Value) (*endpointResponse, error) {
	endpoint := c.Url + "/_sql"

	query := &endpointQuery{
		Stmt: stmt,
	}

	if len(args) > 0 {
		query.Args = args
	}

	buf, err := json.Marshal(query)

	if err != nil {
		return nil, err
	}

	data := bytes.NewReader(buf)

	resp, err := http.Post(endpoint, "application/json", data)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	// Parse response
	res := &endpointResponse{}
	err = json.Unmarshal(body, &res)

	if err != nil {
		return nil, err
	}

	// Check for db errors
	if res.Error.Code != 0 {
		err = errors.New(res.Error.Message)
		return nil, err
	}

	return res, nil
}

// Queries the database
func (c *CrateDriver) Query(stmt string, args []driver.Value) (driver.Rows, error) {
	res, err := c.query(stmt, args)

	if err != nil {
		return nil, err
	}

	// Rows reader
	rows := &Rows{
		columns: res.Cols,
		values: res.Rows,
		rowcount: res.Rowcount,
	}

	return rows, nil
}

// Exec queries on the dataabase
func (c *CrateDriver) Exec(stmt string, args []driver.Value) (result driver.Result, err error) {
	res, err := c.query(stmt, args)

	if err != nil {
		return nil, err
	}

	result = &Result{res.Rowcount}

	return result, nil
}

// Result interface
type Result struct {
	affectedRows int64
}

// Last inserted id
func (r *Result) LastInsertId() (int64, error) {
	err := errors.New("LastInsertId() not supported.")
	return 0, err
}

// # of affected rows on exec
func (r *Result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}


// Rows reader
type Rows struct {
	columns []string
	values [][]interface{}
	rowcount int64
	pos int64 // index position on the values array
}

// Row columns
func (r *Rows) Columns() []string {
	return r.columns
}

// Get the next row
func (r *Rows) Next(dest []driver.Value) error {
	if r.pos >= r.rowcount {
		return io.EOF
	}

	for i := range dest {
		dest[i] = r.values[r.pos][i]
	}

	r.pos++

	return nil
}

// Close
func (r *Rows) Close() error {
	r.pos = r.rowcount // Set to end of list
	return nil
}

// Yet not supported
func (c *CrateDriver) Begin() (driver.Tx, error) {
	err := errors.New("Begin() not supported")
	return nil, err
}

func (c *CrateDriver) Close() error {
	return nil
}

func (c *CrateDriver) Prepare(query string) (driver.Stmt, error) {
	err := errors.New("Prepare() not supported")
	return nil, err
}


// Register the driver
func init() {
	sql.Register("crate", &CrateDriver{})
}
