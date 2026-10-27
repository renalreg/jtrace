-- JTrace version 1.4.5
-- Convert fixed-width character fields to VARCHAR and remove padding.

BEGIN;

UPDATE jtrace.masterrecord
   SET nationalidtype = rtrim(nationalidtype);

UPDATE jtrace.tracerequest
   SET traceid = rtrim(traceid),
       tracetype = rtrim(tracetype),
       nameswap = rtrim(nameswap),
       localid = rtrim(localid),
       localidtype = rtrim(localidtype),
       gender = rtrim(gender),
       postcode = rtrim(postcode);

UPDATE jtrace.traceresponse
   SET traceid = rtrim(traceid),
       tracestarttime = rtrim(tracestarttime),
       traceendtime = rtrim(traceendtime),
       status = rtrim(status);

UPDATE jtrace.traceresponseline
   SET traceid = rtrim(traceid),
       gender = rtrim(gender),
       postcode = rtrim(postcode);

ALTER TABLE jtrace.masterrecord
    ALTER COLUMN nationalidtype TYPE character varying(5)
    USING rtrim(nationalidtype);

ALTER TABLE jtrace.tracerequest
    ALTER COLUMN traceid TYPE character varying(36) USING rtrim(traceid),
    ALTER COLUMN tracetype TYPE character varying(10) USING rtrim(tracetype),
    ALTER COLUMN nameswap TYPE character varying(1) USING rtrim(nameswap),
    ALTER COLUMN localid TYPE character varying(10) USING rtrim(localid),
    ALTER COLUMN localidtype TYPE character varying(5) USING rtrim(localidtype),
    ALTER COLUMN gender TYPE character varying(2) USING rtrim(gender),
    ALTER COLUMN postcode TYPE character varying(10) USING rtrim(postcode);

ALTER TABLE jtrace.traceresponse
    ALTER COLUMN traceid TYPE character varying(36) USING rtrim(traceid),
    ALTER COLUMN tracestarttime TYPE character varying(23) USING rtrim(tracestarttime),
    ALTER COLUMN traceendtime TYPE character varying(23) USING rtrim(traceendtime),
    ALTER COLUMN status TYPE character varying(10) USING rtrim(status);

ALTER TABLE jtrace.traceresponseline
    ALTER COLUMN traceid TYPE character varying(36) USING rtrim(traceid),
    ALTER COLUMN gender TYPE character varying(2) USING rtrim(gender),
    ALTER COLUMN postcode TYPE character varying(10) USING rtrim(postcode);

COMMIT;
