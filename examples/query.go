package main

import "github.com/herenow/go-crate"
import "fmt"

func main() {
	db, err := crate.Open("http://127.0.0.1:4200/")

    if err != nil {
        fmt.Errorf("Error: %v", err)
        return
    }

    response, err := db.Query("SELECT * FROM sample")

    if err != nil {
        fmt.Println(err)
        return
    }

    fmt.Println(response)
}
