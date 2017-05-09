package crate

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	typeTime = "11"
)

// Crate conn structure
type CrateDriver struct {
	Url string // Crate http endpoint url
}

// Init a new "Connection" to a Crate Data Storage instance.
// Note that the connection is not tested until the first query.
func (c *CrateDriver) Open(crate_url string) (driver.Conn, error) {
	u, err := url.Parse(crate_url)

	if err != nil {
		return nil, err
	}

	sanUrl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	c.Url = sanUrl

	return c, nil
}

// JSON endpoint response struct
// We expect error to be null or ommited
type endpointResponse struct {
	Error struct {
		Message string
		Code    int
	} `json:"error"`
	Cols        []string        `json:"cols"`
	Duration    float64         `json:"duration"`
	ColumnTypes []interface{}   `json:"col_types"`
	Rowcount    int64           `json:"rowcount"`
	Rows        [][]interface{} `json:"rows"`
}

// JSON endpoint request struct
type endpointQuery struct {
	Stmt string         `json:"stmt"`
	Args []driver.Value `json:"args,omitempty"`
}

// Query the database using prepared statements.
// Read: https://crate.io/docs/stable/sql/rest.html for more information about the returned response.
// Example: crate.Query("SELECT * FROM sys.cluster LIMIT ?", 10)
// "Parameter Substitution" is also supported, read, https://crate.io/docs/stable/sql/rest.html#parameter-substitution
// This is the internal query function
func (c *CrateDriver) query(stmt string, args []driver.Value) (*endpointResponse, error) {
	endpoint := c.Url + "/_sql?types"

	query := &endpointQuery{
		Stmt: stmt,
	}

	if l:=len(args); l > 0 {
		query.Args = args
		//Process each column that needs conversion from time.Time to int64 for Crate
		for i:= 0; i<l; i++ {
			if val, ok := args[i].(time.Time) ; ok {
				if val.IsZero() {
					query.Args[i] = 0
				} else {
					query.Args[i] = val.UnixNano() / 1000000
				}
			}
		}
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

	// Parse response
	res := &endpointResponse{}
	d := json.NewDecoder(resp.Body)

	// We need to set this, or long integers will be interpreted as floats
	d.UseNumber()

	err = d.Decode(res)

	if err != nil {
		return nil, err
	}

	// Check for db errors
	if res.Error.Code != 0 {
		err = &CrateErr{
			Code:    res.Error.Code,
			Message: res.Error.Message,
		}
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
		columns:  res.Cols,
		values:   res.Rows,
		rowcount: res.Rowcount,
		isTime:   make([]bool, len(res.ColumnTypes)),
	}
	tcount := len(res.ColumnTypes)
	for i:=0; i<tcount; i++ {
		n, ok := res.ColumnTypes[i].(json.Number)
		rows.isTime[i] = (ok && (n.String() == typeTime)) //Probably faster than getting the int64 value of n ...
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
	columns  []string
	values   [][]interface{}
	isTime   []bool	//Flags columns to convert to time.Time (type 11)
	rowcount int64
	pos      int64 // index position on the values array
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
		if (r.isTime[i] && (r.values[r.pos][i] != nil)) { //If column is flagged as time.Time then convert the int64 value to time.Time
			if val, ok := r.values[r.pos][i].(json.Number); ok {
				v , _ := val.Int64()
				sec := v/int64(1000)
				dest[i] = time.Unix(sec, (v-sec*int64(1000))*int64(1000000))
			} else {
				return errors.New(fmt.Sprintf("Failed to convert column %s=%T to time\n", r.columns[i], r.values[r.pos][i]))
			}
		} else {
			dest[i] = r.values[r.pos][i]
		}
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
	err := errors.New("Transactions are not supported by this driver.")
	return nil, err
}

// Nothing to close, crate is stateless
func (c *CrateDriver) Close() error {
	return nil
}

// Prepared stmt interface
type CrateStmt struct {
	stmt   string // Query stmt
	driver *CrateDriver
}

// Driver method that initiates the prepared stmt interface
func (c *CrateDriver) Prepare(query string) (driver.Stmt, error) {
	stmt := &CrateStmt{
		stmt:   query,
		driver: c,
	}

	return stmt, nil
}

// Just pass it to the driver's' default Query() function
func (s *CrateStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.driver.Query(s.stmt, args)
}

// Just pass it to the driver's default Exec() function
func (s *CrateStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.driver.Exec(s.stmt, args)
}

// No need to implement close
func (s *CrateStmt) Close() error {
	return nil
}

// The NumInput method is not supported, return -1 so the database/sql packages knows.
func (s *CrateStmt) NumInput() int {
	return -1
}



// Register the driver
func init() {
	sql.Register("crate", &CrateDriver{})
}
