# DBFGO2MYSQL - Squeeze69

## DBF to MySql program written in [GO](https://golang.org)

[![Build Status](https://travis-ci.org/squeeze69/dbfgo2mysql.svg?branch=master)](https://travis-ci.org/squeeze69/dbfgo2mysql)

### License: GPLv3

**BTW**: Credits to "Squeeze69" would be appreciated if you use this code.

**There is no link between this code and "dbfgo".**

**NOTICE**: USE THIS CODE AT YOUR OWN RISK, NO WARRANTIES!

This could cause the end of the universe, or, worst, some bureaucratic nightmares (just kidding).

To get, this should work: go get github.com/squeeze69/dbfgo2mysql

Build: go build dbfgo2mysql.go

The name should be read: DBF, please, GO TO MYSQL! (DBF GO 2 MYSQL)

It's a simple tool to import dbf files into mysql/mariadb tables, optionally creating them.

BTW: It's absolutly not as complete as the dbf2mysql written in C, so, if you need more options, please, go and find it.
Usage:

```bash
dbfgo2mysql [flags...] profile_file dbffile table
```

The profile_file is, actually, a simple text file, the first line is a standard go sql DSN,
only the first line is used, but it could change in the future.
i.e.:

```text
user:password@(127.0.0.1:3307)/database
```

If you need further details, please take a look [here](https://github.com/go-sql-driver/mysql/#dsn-data-source-name).

Of Course: The user MUST have grants to perform actions.

The flags, up to now, are:

-abortonsqlerror : abort insertion whenever an sql error occur, **note**: error 1114 table full is an exception, it always cause an abort.

-truncate : truncate table before writing

-create : create table - actually, some guesswork goes on to build the "create table ..." statement.
    The "int" types are reduced to their bare minumum for storage (i.e.: 1 digit = TINYINT,etc..)

-dumpcreatetable : dumps on stdout the create table code and exit.

-engine=...[default: MyIsam - if left empty - no Engine="..." is used]

-collate=...[default: utf8_general_ci - if left empty - no Collate="..." is used]

-nobigint : DONT'T use BIGINT type, even if there are more than 9 digits

-insertignore : The "INSERT IGNORE" statement is used instead of "INSERT ", meaning this, if there is duplicate primary key, the error
    doesn't stop the import.

-index=fieldname : Create an index on a specified field.

-firstrecord=number (0 based), record to start import from

-m=number of records to import, starting from the "firstrecord" record.

-drop drop table before every action (create,truncate, insert).

-verbose : verbose output

-g : number of goroutines, default is 2 (or max number of cores, if it's lower), max is an hard coded 64.
-q : number of records in the channel queue, default is 100, minimum is, at least equal to the number of goroutines).

Please, feel free to contact me for bug,suggestions and so on.

Suggested development environment: [Atom](https://atom.io) with the "go-plus" package or [VScode](https://code.visualstudio.com/) with the "GO" extension.
