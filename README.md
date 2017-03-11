DBFGO2MYSQL - Squeeze69
Should be read: DBF, please, GO TO MYSQL!

It's a simple tool to import dbf files into mysql/mariadb tables, optionally creating them.

BTW: It's absolutly not as complete as the dbf2mysql written in C, so, if you need more options, please, go and find it.
Usage:
dbfgo2mysql [flags...] profile_file dbffile table

The flags, up to now, are:
-truncate
	truncate table before writing
-create
	create table - actually, some guesswork goes on to build the "create table ..." statement.
-engine=...[default: MyIsam]
-collate=...[default: utf8_general_ci]

-m=number of records to import, starting from the first.

-verbose
	verbose output


Please, feel free to contact me for bug,suggestions and so on.
