//conversion from dbf to mysql
package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/squeeze69/dbf"
)

//global mysqlurl - see the go lang database/sql package
//sample nopwd url: "root:@(127.0.0.1:3306)/database"
var mysqlurl string
var verbose bool
var maxrecord int
var truncate bool
var createtable bool

//global variables for --create
var collate = "utf8_general_ci"
var engine = "MyIsam"

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

//returns a "CREATE TABLE" string
func createtablestring(table string, collate string, engine string, dbr *dbf.Reader) string {
	var fieldtype string
	arf := make([]string, 0, 200)
	fields := dbr.FieldNames()
	for k := range fields {
		dbfld, _ := dbr.FieldInfo(k)
		switch dbfld.Type {
		case 'D':
			fieldtype = "DATE"
		case 'L': //logical
			fieldtype = "CHAR(1)"
		case 'C': //CHAR
			fieldtype = fmt.Sprintf("VARCHAR(%d)", dbfld.Len)
		case 'N': //Numeric could be either Int or fixed point decimal
			if dbfld.DecimalPlaces > 0 {
				//a VARCHAR will do it, +2 it's for sign and decimal sep.
				fieldtype = fmt.Sprintf("VARCHAR(%d)", dbfld.Len+2)
			} else {
				fieldtype = fmt.Sprintf("INT(%d)", dbfld.Len)
			}
		default:
			fieldtype = "VARCHAR(254)"
		}

		arf = append(arf, fmt.Sprintf("`%s` %s", dbf.Tillzero(dbfld.Name[:]), fieldtype))
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n%s\n)\nCOLLATE='%s' ENGINE = %s;",
		table, strings.Join(arf, ",\n"), collate, engine)
}

func main() {
	var rec dbf.OrderedRecord
	var qstring string
	var skipped, inserted int
	placeholder := make([]string, 0, 200) //preallocate

	var memst runtime.MemStats

	flag.BoolVar(&verbose, "v", false, "verbose output")
	flag.BoolVar(&truncate, "truncate", false, "truncate table before writing")
	flag.IntVar(&maxrecord, "m", -1, "maximum number of records to read")
	flag.StringVar(&collate, "collate", "utf8_general_ci", "Collate to use with CREATE TABLE")
	flag.StringVar(&engine, "engine", "MyIsam", "Engine to use with CREATE TABLE")
	flag.BoolVar(&createtable, "create", false, "Switch to force TABLE CREATION")
	flag.Parse()

	argl := flag.Args()
	if len(argl) < 3 {
		fmt.Println("Usage: dbfgo2mysql [-v] [-m=maxrecords] [--truncate] profile dbffile table")
		os.Exit(1)
	}
	err := readprofile(argl[0])
	if err != nil {
		log.Fatal("Error!!:", err)
	}

	db, err := sql.Open("mysql", mysqlurl)
	if err != nil {
		log.Fatal("Errore!", err)
	}
	defer db.Close()
	inpf, err := os.Open(argl[1])
	if err != nil {
		log.Fatal("dbf file open:", err)
	}
	defer inpf.Close()
	dbfile, err := dbf.NewReader(inpf)
	if err != nil {
		log.Fatal("dbf newreader:", err)
	}
	dbfile.SetFlags(dbf.FlagDateAssql | dbf.FlagSkipWeird | dbf.FlagSkipDeleted)

	if createtable {
		if verbose {
			fmt.Println("Creating Table: ", argl[2])
		}
		_, erc := db.Exec(createtablestring(argl[2], collate, engine, dbfile))
		if erc != nil {
			log.Fatal("CREATE TABLE:", erc)
		}
	}

	fields := dbfile.FieldNames()
	for i := 0; i < len(fields); i++ {
		placeholder = append(placeholder, "?")
	}
	if truncate {
		_, err = db.Exec(fmt.Sprintf("TRUNCATE `%s`;", argl[2]))
		if err != nil {
			log.Fatal("Error truncating:", err)
		}
	}
	qstring = fmt.Sprintf("INSERT INTO %s (`%s`) VALUES (%s);", argl[2], strings.Join(fields, "`,`"), strings.Join(placeholder, ","))
	if verbose {
		fmt.Println("QSTRING:", qstring)
	}
	// retrieve field names
	stmt, err := db.Prepare(qstring)
	if err != nil {
		log.Fatal("Errore! Preparazione statement:", err, "\n", qstring)
	}
	defer stmt.Close()

	if verbose {
		fmt.Println("Number of dbf records:", dbfile.Length)
	}

	for i := 0; i < dbfile.Length; i++ {
		if maxrecord >= 0 && i >= maxrecord {
			break
		}
		rec, err = dbfile.ReadOrdered(i)
		if err == nil {
			if verbose {
				fmt.Println(rec)
			}
			_, err1 := stmt.Exec(rec...)
			if err1 != nil {
				log.Fatal("Errore: stmt.Exec:record:", i, " of ", dbfile.Length, "Error:", err1)
			}
			inserted++
		} else {
			_, ok := err.(*dbf.SkipError)
			if ok {
				skipped++
				continue
			}
			log.Fatal("Loop Error: record:", i, " of ", dbfile.Length, " Error:", err)
		}
	}
	runtime.ReadMemStats(&memst)
	fmt.Printf("Records: Inserted: %d Skipped: %d\n", inserted, skipped)
	fmt.Println("Allocato Totale (KiB): ", memst.TotalAlloc/1024)

}
