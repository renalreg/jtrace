\set ON_ERROR_STOP on
BEGIN;

DO $$
BEGIN
    IF current_database() <> 'jtrace_migration_test' THEN
        RAISE EXCEPTION 'Run this test only in the dedicated jtrace_migration_test database';
    END IF;
END;
$$;

DROP SCHEMA IF EXISTS jtrace CASCADE;
CREATE SCHEMA jtrace;

-- Minimal pre-upgrade fixtures for every column affected by length migrations.
CREATE TABLE jtrace.masterrecord (nationalid varchar(10), nationalidtype char(5));
CREATE TABLE jtrace.person (nationalid varchar(10), localid varchar(10), nationalidtype varchar(5));
CREATE TABLE jtrace.pidxref (localid varchar(17));
CREATE TABLE jtrace.audit (mainnationalid varchar(10), mainnationalidtype varchar(5), updatedby varchar(50));
CREATE TABLE jtrace.linkrecord (updatedby varchar(50));
CREATE TABLE jtrace.workitem (updatedby varchar(50));
CREATE TABLE jtrace.tracerequest (
    traceid char(36), tracetype char(10), nameswap char(1), localid char(10),
    localidtype char(5), gender char(2), postcode char(10)
);
CREATE TABLE jtrace.traceresponse (
    traceid char(36), tracestarttime char(23), traceendtime char(23), status char(10)
);
CREATE TABLE jtrace.traceresponseline (traceid char(36), gender char(2), postcode char(10));
INSERT INTO jtrace.masterrecord VALUES ('123', 'NHS');
INSERT INTO jtrace.tracerequest (traceid, localid) VALUES ('trace-1', '123');

\ir ../SCRIPTS/1.4.2.SQL
\ir ../SCRIPTS/1.4.3.SQL
\ir ../SCRIPTS/1.4.5.SQL
\ir ../SCRIPTS/1.4.7.SQL

DO $$
DECLARE
    field record;
    actual_width integer;
    actual_type text;
BEGIN
    FOR field IN SELECT * FROM (VALUES
        ('masterrecord', 'nationalid', 50), ('masterrecord', 'nationalidtype', 50),
        ('person', 'nationalid', 50), ('person', 'localid', 50), ('person', 'nationalidtype', 50),
        ('pidxref', 'localid', 50), ('audit', 'mainnationalid', 50), ('audit', 'mainnationalidtype', 50),
        ('audit', 'updatedby', 320), ('linkrecord', 'updatedby', 320), ('workitem', 'updatedby', 320),
        ('tracerequest', 'traceid', 36), ('tracerequest', 'tracetype', 10), ('tracerequest', 'nameswap', 1),
        ('tracerequest', 'localid', 10), ('tracerequest', 'localidtype', 5),
        ('tracerequest', 'gender', 2), ('tracerequest', 'postcode', 10),
        ('traceresponse', 'traceid', 36), ('traceresponse', 'tracestarttime', 23),
        ('traceresponse', 'traceendtime', 23), ('traceresponse', 'status', 10),
        ('traceresponseline', 'traceid', 36), ('traceresponseline', 'gender', 2), ('traceresponseline', 'postcode', 10)
    ) AS fields(table_name, column_name, expected_width)
    LOOP
        SELECT character_maximum_length, data_type INTO STRICT actual_width, actual_type
          FROM information_schema.columns
         WHERE table_schema = 'jtrace'
           AND table_name = field.table_name AND column_name = field.column_name;
        IF actual_width IS DISTINCT FROM field.expected_width OR actual_type <> 'character varying' THEN
            RAISE EXCEPTION 'Unexpected definition for %.%: % (%)',
                field.table_name, field.column_name, actual_type, actual_width;
        END IF;
    END LOOP;
    IF (SELECT nationalidtype FROM jtrace.masterrecord) <> 'NHS'
       OR (SELECT localid FROM jtrace.tracerequest) <> '123' THEN
        RAISE EXCEPTION 'CHAR padding was not removed';
    END IF;
END;
$$;

UPDATE jtrace.masterrecord SET nationalidtype = 'UKRR_UID';
UPDATE jtrace.tracerequest SET localidtype = 'MRN';
INSERT INTO jtrace.person (nationalidtype) VALUES ('UKRR_UID');
INSERT INTO jtrace.audit (mainnationalidtype) VALUES ('UKRR_UID');
ALTER TABLE jtrace.masterrecord ALTER COLUMN nationalid TYPE varchar(100);
ALTER TABLE jtrace.masterrecord ALTER COLUMN nationalidtype TYPE varchar(100);
ALTER TABLE jtrace.person ALTER COLUMN localid TYPE varchar(100);
ALTER TABLE jtrace.person ALTER COLUMN nationalidtype TYPE varchar;
ALTER TABLE jtrace.tracerequest ALTER COLUMN traceid TYPE varchar(80);
ALTER TABLE jtrace.audit ALTER COLUMN mainnationalid TYPE varchar;
ALTER TABLE jtrace.audit ALTER COLUMN updatedby TYPE text;
UPDATE jtrace.masterrecord SET nationalid = repeat('X', 80);
UPDATE jtrace.person SET localid = repeat('Y', 80);
UPDATE jtrace.tracerequest SET traceid = repeat('Z', 70);
UPDATE jtrace.audit SET mainnationalid = repeat('A', 120), updatedby = repeat('U', 400);

-- Re-running earlier conversions must not undo later/deployment-specific widening.
\ir ../SCRIPTS/1.4.2.SQL
\ir ../SCRIPTS/1.4.3.SQL
\ir ../SCRIPTS/1.4.5.SQL
\ir ../SCRIPTS/1.4.7.SQL

DO $$
BEGIN
    IF (SELECT length(nationalid) FROM jtrace.masterrecord) <> 80
       OR (SELECT nationalidtype FROM jtrace.masterrecord) <> 'UKRR_UID'
       OR (SELECT length(localid) FROM jtrace.person) <> 80
       OR (SELECT length(traceid) FROM jtrace.tracerequest) <> 70
       OR (SELECT length(mainnationalid) FROM jtrace.audit) <> 120
       OR (SELECT length(updatedby) FROM jtrace.audit) <> 400 THEN
        RAISE EXCEPTION 'A repeated migration changed identifier contents';
    END IF;
    IF (SELECT character_maximum_length FROM information_schema.columns
        WHERE table_schema = 'jtrace' AND table_name = 'masterrecord' AND column_name = 'nationalid') <> 100
       OR (SELECT character_maximum_length FROM information_schema.columns
        WHERE table_schema = 'jtrace' AND table_name = 'tracerequest' AND column_name = 'traceid') <> 80
       OR (SELECT character_maximum_length FROM information_schema.columns
        WHERE table_schema = 'jtrace' AND table_name = 'masterrecord' AND column_name = 'nationalidtype') <> 100
       OR (SELECT character_maximum_length FROM information_schema.columns
        WHERE table_schema = 'jtrace' AND table_name = 'person' AND column_name = 'nationalidtype') IS NOT NULL
       OR EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'jtrace'
          AND table_name = 'audit' AND column_name IN ('mainnationalid', 'updatedby')
          AND character_maximum_length IS NOT NULL) THEN
        RAISE EXCEPTION 'A repeated migration reduced column capacity';
    END IF;
END;
$$;

ROLLBACK;
