package parser

import (
	// "database/sql"
	"fmt"
	"os"
	"strings"

	// _ "github.com/go-sql-driver/mysql"
)

// func main() {
// 	query := parseQuery(os.Args[1:])
// 	fmt.Println(query)
// 	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/tychy")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer db.Close()
// 	rows, err := db.Query(query)
// 	if err != nil{
// 		panic(err)
// 	}
// 	columns, err := rows.Columns()
// 	for i := range columns{
// 		println(columns[i])
// 	}
// }

func parseQuery(cargs []string) string{
	if cargs[0] != "select" {
		fmt.Println("不正なクエリです")
		os.Exit(1)
	}

	queryArguments := cargs[1:]
	for i := range queryArguments {
		if queryArguments[i] != "id" && queryArguments[i] != "name" && queryArguments[i] != "description" {
			fmt.Println("不正なカラム名です")
			os.Exit(1)
		}
	}
	columns := strings.Join(queryArguments, ", ")
	query := "SELECT " + columns + " FROM projects"
	return query
}
