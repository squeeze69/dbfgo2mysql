// prove con sql in go
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"

	_ "github.com/go-sql-driver/mysql"
	"github.com/squeeze69/dbf"
)

//global mysqlurl - see the go lang database/sql package
var mysqlurl string

//read profile, actually a fixed position file, first row it's a sql url
func readprofile(prfname string) error {
	s := make([]string, 0, 4)
	f, err := os.Open(prfname)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s = append(s, scanner.Text())
	}
	mysqlurl = s[0]
	return nil
}

func main() {
	var rec dbf.Record
	var memst runtime.MemStats
	if len(os.Args) < 4 {
		fmt.Println("Usage: dbfgo2mysql profile dbffile table")
		os.Exit(1)
	}
	err := readprofile(os.Args[0])
	if err != nil {
		log.Fatal("Error!!:", err)
	}

	db, err := sql.Open("mysql", "root:@(127.0.0.1:3307)/rmp")
	if err != nil {
		log.Fatal("Errore!", err)
	}
	defer db.Close()
	inpf, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal("dbf file open:", err)
	}
	defer inpf.Close()
	dbfile, err := dbf.NewReader(inpf)
	if err != nil {
		log.Fatal("dbf newreader:", err)
	}
	dbfile.SetFlags( dbf.FlagDateAssql)
	// retrieve field names
	
	stmt, err := db.Prepare("SELECT ID AS IDART,CODICE_AR,DESCRI_AR FROM articoli WHERE CODICE_CL = ?")
	if err != nil {
		log.Fatal("Errore!", err)
	}
	defer stmt.Close()

	righe, err := stmt.Query(os.Args[1])
	if err != nil {
		log.Fatal("Errore!", err)
	}

	runtime.ReadMemStats(&memst)
	fmt.Println("Allocato Totale (KiB): ", memst.TotalAlloc/1024)
}
