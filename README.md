DBFGO2MYSQL - Squeeze69

License: GPLv3

BTW: Credits to "Squeeze69" would be appreciated if you use this code.

NOTICE: USE THIS CODE AT YOUR OWN RISK, NO WARRANTIES!

This could cause the end of the universe, or, worst, some buraucratic nightmares.

Use this at your own risk.

Build: go build dbfgo2mysql.go

Should be read: DBF, please, GO TO MYSQL! (DBF GO 2 MYSQL)

It's a simple tool to import dbf files into mysql/mariadb tables, optionally creating them.

BTW: It's absolutly not as complete as the dbf2mysql written in C, so, if you need more options, please, go and find it.
Usage:
dbfgo2mysql [flags...] profile_file dbffile table

The profile_file is, actually, a simple text file, the first line is a standard go sql DSN,
only the first line is used, but it could change in the future.
i.e.:
user:password@(127.0.0.1:3307)/database

see here ( https://github.com/go-sql-driver/mysql/#dsn-data-source-name ).

Of Course: The user MUST have grants to perform actions.

The flags, up to now, are:
-truncate
	truncate table before writing
-create
	create table - actually, some guesswork goes on to build the "create table ..." statement.
	The "int" types are reduced to their bare minumum for storage (i.e.: 1 digit = TINYINT,etc..)
-engine=...[default: MyIsam]
-collate=...[default: utf8_general_ci]
-nobigint
	DONT'T use BIGINT type, even if there are more than 9 digits

-inserignore
	The "INSERT IGNORE" statement is used instead of "INSERT ", meaning this, if there is duplicate primary key, the error
	doesn't stop the import.

-m=number of records to import, starting from the first.

-drop drop table before every action (create,truncate, insert).

-verbose
	verbose output



Please, feel free to contact me for bug,suggestions and so on.
