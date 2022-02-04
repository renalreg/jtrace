# JTrace

EMPI extension for Mirth Connect, used in the UKRDC

## Packages and Builds

Tagged releases are published to the GitHub Package Registry.

Every commit to this repository will be built and stored as an artifact for 30 days. 
This is to allow for easy access to the latest builds for testing purposes.

## Local development

### Requirements

* Java JDK 8
* Maven

### Running tests

Running the included unit tests requires a PostgreSQL database to be running locally.
The database must be running on port 5432, with a username of `postgres` and a password of `postgres`.

To set up/reset the database, run the following commands in a terminal window:

```
export PGPASSWORD=postgres
createuser -h localhost -p 5432 -U postgres ukrdc
psql -h localhost -p 5432 -U postgres -f Database/DDL/JTRACE.DDL
for s in Database/DDL/*.SQL; do echo "Running script ${s}"; psql -h localhost -p 5432 -U postgres JTRACE -f $s; done
mvn test
```
