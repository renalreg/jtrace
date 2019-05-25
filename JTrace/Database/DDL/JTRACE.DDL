-- Database: "JTRACE"
DROP DATABASE "JTRACE";

CREATE DATABASE "JTRACE"
  WITH OWNER = postgres
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       CONNECTION LIMIT = -1;

-- Connect to the database
\c JTRACE;

DROP SCHEMA "jtrace" CASCADE;

CREATE SCHEMA "jtrace"
  AUTHORIZATION postgres;

GRANT ALL ON SCHEMA "jtrace" TO ukrdc;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA jtrace TO ukrdc;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA jtrace TO ukrdc;

ALTER ROLE ukrdc SET search_path TO extract, jtrace;
