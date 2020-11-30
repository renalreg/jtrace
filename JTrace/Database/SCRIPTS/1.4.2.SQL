-- JTrace version 1.4.2
-- 28/11/2020

-- Master Record
ALTER TABLE jtrace.masterrecord ALTER COLUMN nationalid type character varying(50);

-- PIDXREF
ALTER TABLE jtrace.pidxref ALTER COLUMN localid type character varying(50);

-- PERSON
ALTER TABLE jtrace.person ALTER COLUMN nationalid type character varying(50);
ALTER TABLE jtrace.person ALTER COLUMN localid type character varying(50);

-- AUDIT
ALTER TABLE jtrace.audit ALTER COLUMN updatedby type character varying(320);

-- LINKRECORD
ALTER TABLE jtrace.linkrecord ALTER COLUMN updatedby type character varying(320);

-- WORKITEM
ALTER TABLE jtrace.workitem ALTER COLUMN updatedby type character varying(320);
