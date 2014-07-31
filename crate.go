package crate

import (
	"net/http"
    "net/url"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "bytes"
)

// Crate conn structure
type CrateConn struct {
    host string // Host:port
    url string // Final url
}

// Crate json query struct
type Query struct {
    Stmt string `json:"stmt"`
    Args []interface{} `json:"args,omitempty"`
}

// Init a new "Connection" to a Crate Data Storage instance.
// Note that the connection is not tested until the first query.
// crate_url example: http://localhost:4200/
func Open(crate_url string) (c CrateConn, err error) {
    u, err := url.Parse(crate_url)

    if err != nil {
        return
    }

    sanUrl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

    c.host = u.Host
    c.url = sanUrl

	return
}

// Query the database using prepared statements.
// Note that this function will simply return a json string from crate's http endpoint.
// You will still need to check the json response for sql errors.
// Read: https://crate.io/docs/stable/sql/rest.html for more information about the returned response.
// Example: crate.Query("SELECT * FROM sys.cluster LIMIT ?", 10)
func (c *CrateConn) Query(stmt string, args ...interface{}) (string, error) {
    endpoint := c.url + "/_sql"

    query := &Query{
        Stmt: stmt,
        Args: args,
    }

    buf, err := json.Marshal(query)

	if err != nil {
        return "", err
	}

    data := bytes.NewReader(buf)

    resp, err := http.Post(endpoint, "application/json", data)

    if err != nil {
        return "", err
    }

    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)

    if err != nil {
        return "", err
    }

	return string(body), nil
}
