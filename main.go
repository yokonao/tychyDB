package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/tychy")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	hoge, err := db.Query("SHOW TABLES")
	if err != nil{
		panic(err)
	}
	columns, err := hoge.Columns()
	for i := range columns{
		println(columns[i])
	}
}
