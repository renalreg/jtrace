# Database DDL and Scripts

## Reset database

* Open up a terminal session from Jtrace/Database/DDL in Eclipse

* Login into postgres 

`psql -U postgres`

Enter the password (`postgres`)

* Rebuild the db and schema

`\i JTRACE.DDL`

* Rebuild all the tables

`\i REBUILD_ALL_TABLES.DDL`
