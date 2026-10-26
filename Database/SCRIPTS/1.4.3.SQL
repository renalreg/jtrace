-- JTrace version 1.4.3
-- Allow incoming PatientNumber identifiers to contain up to 50 characters.

ALTER TABLE jtrace.masterrecord
    ALTER COLUMN nationalid TYPE character varying(50);

ALTER TABLE jtrace.person
    ALTER COLUMN nationalid TYPE character varying(50);

ALTER TABLE jtrace.audit
    ALTER COLUMN mainnationalid TYPE character varying(50);
