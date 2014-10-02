go-crate
========

Golang Sql Driver for Crate Data Storage. (https://crate.io/)

(http://godoc.org/github.com/herenow/go-crate)[godoc.org/github.com/herenow/go-crate]
(http://golang.org/pkg/database/sql/)[http://golang.org/pkg/database/sql/]


Install & Usage
--------
```
go get github.com/herenow/go-crate
```

```golang
import "database/sql"
import _ "github.com/herenow/go-crate"
```


Not Supported SQL Functions
------

Some functions of the `database/sql` package may not be supported, due to a lack of support of Crate or this package.
`Transactions` are not supported by crate, and some interfaces like `Stmt` have not yet been implemented on this driver.


Notes
-----
* Feel free to send in contributions to this package.
