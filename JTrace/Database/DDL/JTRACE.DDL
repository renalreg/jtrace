-- Database: "JTRACE"
DROP DATABASE "jtrace";

CREATE DATABASE "jtrace"
  WITH OWNER = postgres
       ENCODING = 'UTF8'
       TABLESPACE = pg_default
       CONNECTION LIMIT = -1;

-- Schema: extract
DROP SCHEMA "jtrace" CASCADE;

CREATE SCHEMA "jtrace"
  AUTHORIZATION postgres;

GRANT ALL ON SCHEMA "jtrace" TO ukrdc;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA jtrace TO ukrdc;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA jtrace TO ukrdc;

ALTER ROLE ukrdc SET search_path TO extract, jtrace;
