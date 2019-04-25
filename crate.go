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
	"reflect"
	"strings"
	//"log"
)

// Crate conn structure
type CrateDriver struct {
	Url string // Crate http endpoint url
}

//GeoPoint represents Crate GeoPoint column
type GeoPoint struct {
	Lat float64
	Lon float64
}

//CrateArray represents an Array column type
type CrateArray []interface{}

//crateMap used to store any map and force our own MarshalJSON method to be called
type crateMap map[string]interface{}

//Scan : Implements Scanner interface to populate a GeoPoint when the result is an array of 2 floats
func (gp *GeoPoint) Scan(src interface{}) error {
	if b, ok := src.([]interface{}) ; ok && len(b) == 2 {
		var err error
		if gp.Lon, err = b[0].(json.Number).Float64(); err == nil {
			if gp.Lat, err = b[1].(json.Number).Float64(); err == nil {
				return nil
			}
		}
		return fmt.Errorf("failed to convert %v to GeoPoint : %v", src, err)
	}
	return fmt.Errorf("failed to convert %v to GeoPoint", src)
}

//Scan : Implements Scanner interface to populate a CrateArray from the incoming data
func (arr *CrateArray) Scan(src interface{}) error {
	if srcArr, ok := src.([]interface{}) ; ok  {
		*arr = make ([]interface{}, len(srcArr))
		for  i, obj:=range(srcArr) {
			(*arr)[i] = obj
		}
		return nil
	}
	return fmt.Errorf("failed to convert %v to CrateArray", src)
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

//encodeArray will encode the array represented by obj and store the result in buf
//It returns an error if obj contains a map with keys other than strings
func encodeArray(buf *bytes.Buffer, obj reflect.Value) error {
	m := obj.Len()
	if m == 0 {
		buf.WriteString("[]")
		return nil
	}
	buf.WriteByte('[')
	var k reflect.Kind
	ue := false
	for i:=0; i<m; i++ {
		v := obj.Index(i)
		if i>0 {
			buf.WriteByte(',')
			if ue {
				v = v.Elem()
			}
		} else {
			k = v.Kind()
			if k == reflect.Interface {
				ue = true
				v = v.Elem()
				k = v.Type().Kind()
			}
		}
		switch k {
		case reflect.Float32, reflect.Float64:
			fv := v.Float()
			i := float64(int32(fv))
			if i == fv {
				buf.WriteString(fmt.Sprintf("%0.1f", fv))
				continue
			}
			//Prevents rounding errors seen with floats like 0.01*41 which is 0.41000000000000003 ...
			//See https://floating-point-gui.de/
			buf.WriteString(fmt.Sprintf("%0.6f", fv))
			continue
		case reflect.Map:
			t := reflect.TypeOf(v)
			if v.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("cannot encode map with keys of type %v", t)
			}
			if err := encodeMap(buf, v) ; err != nil {
				return err
			}
			continue
		case reflect.Slice, reflect.Array:
			if err := encodeArray(buf, v); err != nil {
				return err
			}
			continue
		case reflect.String:
			buf.WriteString(fmt.Sprintf("%s", strings.Replace(v.String(), "\"", "\\\"", -1)))
			continue
		}
		buf.WriteString(fmt.Sprintf("%v", v))
	}
	buf.WriteByte(']')
	return nil
}

//encodeMap will encode the map stored in obj in json and store it as a string in the buffer buf
//This is used because one cannot rely on the json encoder because it will format any float with decimal part of 0 as an int
//If the first value to be stored in a new object's key is an int then all further values will be stored as int
//and one will loose the decimal part of each value... Our encoder will ensure that a float with a 0 decimal part
//is encoded as X.0 and not X
//Note it will not encode maps with keys other than strings
func encodeMap(buf *bytes.Buffer, obj reflect.Value) error{
	if obj.Len() == 0 {
		buf.WriteString("{}")
		return nil
	}
	buf.WriteByte('{')
	first := true
	for _, k := range obj.MapKeys() {
		if first {
			first = false
		} else {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("\"%s\":", k))
		fm := "%v"
		v := obj.MapIndex(k).Elem()
		vk := v.Kind()
		if vk == reflect.Interface {
			v = v.Elem()
			vk = v.Type().Kind()
		}
		switch vk {
		case reflect.Float64, reflect.Float32:
			f := v.Float()
			i := float64(int64(f))
			if i == f {
				fm = "%0.1f"
			}
		case reflect.Map:
			t := reflect.TypeOf(v)
			if v.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("cannot encode map with keys of type %v", t)
			}
			if err := encodeMap(buf, v) ; err != nil {
				return err
			}
			continue
		case reflect.Slice, reflect.Array:
			if err := encodeArray(buf, v) ; err != nil {
				return err
			}
			continue
		case reflect.String:
			buf.WriteString(fmt.Sprintf("\"%s\"" , strings.Replace(v.String(), "\"", "\\\"", -1)))
			continue
		}
		buf.WriteString(fmt.Sprintf(fm , v))
	}
	buf.WriteByte('}')
	return nil
}


//MarshalJSON custom JSON marshal function to properly marshall maps containing floats with decimal part equals to 0
func (v crateMap) MarshalJSON() ([]byte, error) {
	res := bytes.Buffer{}
	if err := encodeMap(&res, reflect.ValueOf(v)) ; err != nil {
		return nil, err
	}
	//log.Printf("Result Map : %v", res.String())
	return res.Bytes(), nil
}

//MarshalJSON custom JSON marshal function to properly handle arrays of floats with decimal part equals to 0
func (v CrateArray)  MarshalJSON() ([]byte, error) {
	res := bytes.Buffer{}
	if err := encodeArray(&res, reflect.ValueOf(v)) ; err != nil {
		return nil, err
	}
	//log.Printf("Result Array : %v", res.String())
	return res.Bytes(), nil
}

//CheckNamedValue Convert map, CrateArray, time & GeoPoint arguments to DB format.
func (c *CrateDriver) CheckNamedValue(v *driver.NamedValue) error {
	if obj, ok := v.Value.(map[string]interface{}) ; ok {
		v.Value = crateMap(obj)
		return nil
	} else if ts, ok := v.Value.(time.Time) ; ok {
		if ts.IsZero() {
			v.Value = 0
		} else {
			v.Value = ts.In(time.UTC).UnixNano() / 1000000
		}
		return nil
	} else if gp, ok := v.Value.(GeoPoint) ; ok {
		//fmt.Printf("CheckNamedValue for Geopoint (%f,%f) \n", gp.Lon, gp.Lat)
		nGp := make([]float64, 2)
		nGp[0] = gp.Lon
		nGp[1] = gp.Lat
		v.Value = &nGp
		return nil
	} else if _, ok := v.Value.(CrateArray) ; ok {
		return nil
	} else if arr, ok := v.Value.([]interface{}) ; ok {
		v.Value = CrateArray(arr)
		return nil
	}
	/*else {
		fmt.Printf("CheckNamedValue  for %v -> %v\n", v.Name, v.Value)
	}*/
	return driver.ErrSkip
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
		isSpecial:   make([]int64, len(res.Cols)),
	}
	tcount := len(res.ColumnTypes)
	for i:=0; i<tcount; i++ {
		if n, ok := res.ColumnTypes[i].(json.Number); ok {
			if t, err := n.Int64(); err == nil {
				if t == typeTimestamp || t == typeGeoPoint {
					rows.isSpecial[i] = t
				}
			}
		}
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
	isSpecial   []int64	//Flags columns to convert to time.Time (type 11)
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
		if ((r.isSpecial[i] != 0) && (r.values[r.pos][i] != nil)) {
			if r.isSpecial[i] == typeTimestamp {
				if val, ok := r.values[r.pos][i].(json.Number); ok {
					v, _ := val.Int64()
					sec := v / int64(1000)
					dest[i] = time.Unix(sec, (v-sec*int64(1000))*int64(1000000))
				} else {
					return fmt.Errorf("failed to convert column %s=%T to time", r.columns[i], r.values[r.pos][i])
				}
			} else if r.isSpecial[i] == typeGeoPoint {
				if psrc, ok := r.values[r.pos][i].([]interface{}) ; ok && (len(psrc) == 2) {
					var p GeoPoint
					var err error
					if p.Lon, err = psrc[0].(json.Number).Float64(); err != nil {
						return fmt.Errorf("failed to convert to latitude %v", psrc[0])
					}
					if p.Lat, err = psrc[1].(json.Number).Float64(); err != nil {
						return fmt.Errorf("failed to convert to longitude elem %v", psrc[1])
					}
					dest[i] = &p
				} else if len(psrc) == 0 {
					dest[i] = GeoPoint{}
				} else {
					return fmt.Errorf("failed to convert %v to GeoPoint", r.values[r.pos][i])
				}
			}
		} else {
			dest[i] = r.values[r.pos][i]
			//fmt.Printf("Processing column %s : %+v / %s\n", r.columns[i], r.values[r.pos][i], reflect.TypeOf(r.values[r.pos][i]))
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
