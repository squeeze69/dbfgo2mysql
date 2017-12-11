//conversion from dbf to mysql
//version 0.1.1 (probably forever,I'm a kind of conservative in changing the version)
//written by Squeeze69

package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"text/template"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/squeeze69/dbf"
)

// default values and other constants
const (
	defaultEngine      = "MyIsam"
	defaultCollation   = "utf8_general_ci"
	defaultRecordQueue = 100
	defaultGoroutines  = 2
	maxGoroutines      = 64
	minGoroutines      = 1
	minRecordQueue     = 1
)

//number of records in the queue
var recordQueue = defaultRecordQueue

//number of goroutines spawned
var numGoroutines = defaultGoroutines

//global mysqlurl - see the go lang database/sql package
//sample url: "user:password@(127.0.0.1:3306)/database"
var mysqlurl string

//various flags, set by command line, default to false
var verbose, truncate, createtable, dumpcreatetable, insertignore, nobigint, droptable bool

//max number of record to import, defaults to -1 (means no limit)
var maxrecord int

//global variables for --create
var collate = defaultCollation
var engine = defaultEngine

//LockableCounter a simple counter with a Mutex
type LockableCounter struct {
	count int
	l     sync.Mutex
}

//Increment lockable counter by i items
func (lc *LockableCounter) Increment(i int) {
	lc.l.Lock()
	defer lc.l.Unlock()
	lc.count = lc.count + i
}

//total number on insert errors (if any)
var ierror LockableCounter

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

//returns a "CREATE TABLE" string using templates
func createTableString(table string, collate string, engine string, dbr *dbf.Reader) string {
	var fieldtype string
	fields := dbr.FieldNames()
	//pre allocate
	arf := make([]string, 0, len(fields))
	for k := range fields {
		dbfld, _ := dbr.FieldInfo(k)
		switch dbfld.Type {
		case 'D': //Date field
			fieldtype = "DATE"
		case 'L': //logical
			fieldtype = "CHAR(1)"
		case 'C': //CHAR
			fieldtype = fmt.Sprintf("VARCHAR(%d)", dbfld.Len)
		case 'N': //Numeric could be either Int or fixed point decimal
			if dbfld.DecimalPlaces > 0 {
				//A VARCHAR will do it, +2 it's for sign and decimal sep.
				fieldtype = fmt.Sprintf("VARCHAR(%d)", dbfld.Len+2)
			} else {
				var inttype string
				switch {
				case dbfld.Len < 3:
					inttype = "TINYINT"
				case dbfld.Len >= 3 && dbfld.Len < 5:
					inttype = "SMALLINT"
				case dbfld.Len >= 5 && dbfld.Len < 7:
					inttype = "MEDIUMINT"
				case (dbfld.Len >= 7 && dbfld.Len < 10) || nobigint:
					inttype = "INT"
				case dbfld.Len >= 10:
					inttype = "BIGINT"
				}
				fieldtype = fmt.Sprintf("%s(%d)", inttype, dbfld.Len)
			}
		default:
			fieldtype = fmt.Sprintf("VARCHAR(%d)", dbfld.Len)
		}
		arf = append(arf, fmt.Sprintf("`%s` %s", dbf.Tillzero(dbfld.Name[:]), fieldtype))
	}

	//template for table's creation
	tmpl, err := template.New("table").Parse(
		`CREATE TABLE IF NOT EXISTS {{.Tablename}} (
{{range $i,$e := .Arf}}
{{- if $i}},
{{end}}{{$e}}{{end}}
){{if .Collate}} COLLATE='{{.Collate}}'{{end}}{{if .Engine}} ENGINE='{{.Engine}}'{{end}};`)
	if err != nil {
		log.Fatal(err)
	}
	var str string
	buf := bytes.NewBufferString(str)
	er1 := tmpl.Execute(buf, struct {
		Tablename, Collate, Engine string
		Arf                        []string
	}{Tablename: "`" + table + "`", Collate: collate, Engine: engine, Arf: arf})
	if er1 != nil {
		log.Fatal(er1)
	}
	return buf.String()
}

//Prepare the command line handling
func commandLineSet() {
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	flag.BoolVar(&truncate, "truncate", false, "Truncate table before writing")
	flag.BoolVar(&droptable, "drop", false, "Drop table before anything")
	flag.BoolVar(&insertignore, "insertignore", false, "use 'INSERT IGNORE' instead of INSERT")
	flag.BoolVar(&nobigint, "nobigint", false, "DON'T use BIGINT type, sometimes fields are over-dimensioned")
	flag.IntVar(&maxrecord, "m", -1, "Maximum number of records to read")
	flag.StringVar(&collate, "collate", "utf8_general_ci", "Collate to use with CREATE TABLE (if empty, no collate is specified)")
	flag.StringVar(&engine, "engine", "MyIsam", "Engine to use with CREATE TABLE (if empty, no engine is specified)")
	flag.BoolVar(&createtable, "create", false, "Switch to CREATE TABLE IF NOT EXISTS")
	flag.BoolVar(&dumpcreatetable, "dumpcreatetable", false, "Dump the CREATE TABLE string and exit,no other actions.")
	flag.IntVar(&recordQueue, "q", defaultRecordQueue, "Max record queue")
	flag.IntVar(&numGoroutines, "g", defaultGoroutines, "Max number of insert threads (keep it low...)")
	flag.Parse()
	//enforce limits
	switch {
	case numGoroutines < minGoroutines:
		numGoroutines = minGoroutines
	case numGoroutines > maxGoroutines:
		numGoroutines = maxGoroutines
	}
	if recordQueue < minRecordQueue {
		recordQueue = minRecordQueue
	}
	//a recordQueue less than goroutines is a waste of resources
	if recordQueue < numGoroutines {
		recordQueue = numGoroutines
	}

}

//insertRoutine goroutine to insert data in dbms
func insertRoutine(ch chan dbf.OrderedRecord, over *sync.WaitGroup, stmt *sql.Stmt) {
	defer over.Done()
	defer func() {
		//just respawning go routine in case of error - i.e. bad data are not inserted (i.e. slightly malformed dbf rows)
		if r := recover(); r != nil {
			err, ok := r.(error)
			if ok {
				ierror.Increment(1)
				fmt.Println("Recover:", err)
				over.Add(1)
				go insertRoutine(ch, over, stmt)
			}
		}
	}()
	for i := range ch {
		_, err := stmt.Exec(i...)
		if err != nil {
			panic(err)
		}
	}
	return
}

//workaround for os.Exit non onoring deferred functions
func metamain() (int, string, error) {
	var start = time.Now()
	var qstring string
	var insertstatement = "INSERT"
	var skipped, inserted int

	commandLineSet()

	argl := flag.Args()
	if len(argl) < 3 {
		fmt.Println("Usage: dbfgo2mysql [switches] profile dbffile table")
		fmt.Println("Switches with parameters should be written like: -switch=parameter, i.e.: -g=4")
		flag.PrintDefaults()
		return 0, "", nil
	}

	if err := readprofile(argl[0]); err != nil {
		return 1, "Error:", err
	}

	//open the mysql link
	db, err := sql.Open("mysql", mysqlurl)

	if err != nil {
		return 1, "Error:", err
	}
	defer db.Close()

	inpf, err := os.Open(argl[1])
	if err != nil {
		return 1, "Error: dbf file open:", err
	}
	defer inpf.Close()
	dbfile, err := dbf.NewReader(inpf)
	if err != nil {
		return 1, "Error: dbf newreader:", err
	}
	//Set the some default flags, skips deleted and "weird" records (see dbf package)
	dbfile.SetFlags(dbf.FlagDateAssql | dbf.FlagSkipWeird | dbf.FlagSkipDeleted | dbf.FlagEmptyDateAsZero)

	//check if the table must be dropped before creation
	if droptable && !dumpcreatetable {
		if verbose {
			fmt.Println("Dropping table:", argl[2])
		}
		if _, erd := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", argl[2])); erd != nil {
			return 1, "Error: Dropping:", erd
		}
	}

	//create table section
	if createtable || dumpcreatetable {
		if verbose {
			fmt.Println("Creating Table: ", argl[2])
		}
		ctstring := createTableString(argl[2], collate, engine, dbfile)
		if !dumpcreatetable {
			if _, erc := db.Exec(ctstring); erc != nil {
				return 1, "Error: CREATE TABLE:", erc
			}
		}
		if verbose || dumpcreatetable {
			fmt.Println("-- CREATE TABLE:\n", ctstring)
		}
		if dumpcreatetable {
			return 0, "", nil
		}
	}

	//retrieve fields to build the query
	fields := dbfile.FieldNames()
	placeholder := make([]string, 0, len(fields)) //preallocate to reduce memory fragmentation
	for i := 0; i < len(fields); i++ {
		placeholder = append(placeholder, "?")
	}
	if truncate && !droptable {
		_, err = db.Exec(fmt.Sprintf("TRUNCATE `%s`;", argl[2]))
		if err != nil {
			return 1, "Error: truncating:", err
		}
	}

	//building the code for the prepared statement
	if insertignore {
		insertstatement = "INSERT IGNORE"
	}
	qstring = fmt.Sprintf("%s INTO %s (`%s`) VALUES (%s);", insertstatement, argl[2], strings.Join(fields, "`,`"), strings.Join(placeholder, ","))
	if verbose {
		fmt.Println("QSTRING:", qstring)
	}
	//it's using a prepared statement, much safer and faster
	stmt, err := db.Prepare(qstring)

	if err != nil {
		return 1, "Error: Preparing statement:", err
	}
	defer stmt.Close()

	if verbose {
		fmt.Println("Number of dbf records:", dbfile.Length)
	}

	chord := make(chan dbf.OrderedRecord, recordQueue)
	wgroup := new(sync.WaitGroup)
	for i := 0; i < numGoroutines; i = i + 1 {
		wgroup.Add(1)
		go insertRoutine(chord, wgroup, stmt)
	}
	for i := 0; i < dbfile.Length; i++ {
		if maxrecord >= 0 && i >= maxrecord {
			break
		}

		runtime.Gosched()
		rec, err := dbfile.ReadOrdered(i)
		if err == nil {
			if verbose {
				fmt.Println(rec)
			}
			chord <- rec
			inserted++
		} else {
			if _, ok := err.(*dbf.SkipError); ok {
				skipped++
				continue
			}
			return 1, fmt.Sprint("Error: Loop, record:", i, " of ", dbfile.Length), err
		}
	}
	close(chord)
	//waiting for insertRoutine to end
	wgroup.Wait()
	//printing statistics
	fmt.Printf("Records: Inserted: %d Skipped: %d\nElapsed Time: %s\n",
		inserted, skipped, time.Now().Sub(start))
	fmt.Printf("Queue capacity:%d,goroutines:%d\n",
		recordQueue, numGoroutines)
	if ierror.count > 0 {
		fmt.Printf("Insert Errors:%d\n", ierror.count)
	}
	return 0, "", nil
}

func main() {
	ec, msg, err := metamain()
	if ec != 0 {
		log.Println(msg, err)
	}
	os.Exit(ec)
}
