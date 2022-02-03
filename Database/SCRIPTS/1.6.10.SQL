-- Data Repo version 1.6.10
-- JTrace version 1.3.8
-- 26/11/2019

-- Link record

ALTER TABLE jtrace.linkrecord ADD COLUMN creationdate timestamp without time zone NOT NULL default now();
ALTER TABLE jtrace.linkrecord
ALTER COLUMN lastupdated
SET DEFAULT now();

-- Master Record

ALTER TABLE jtrace.masterrecord ADD COLUMN creationdate timestamp without time zone NOT NULL DEFAULT Now();
ALTER TABLE jtrace.masterrecord
ALTER COLUMN lastupdated
SET DEFAULT Now();

-- Person

ALTER TABLE jtrace.person ADD COLUMN creationdate timestamp without time zone NOT NULL DEFAULT Now();       
ALTER TABLE jtrace.person ADD COLUMN lastupdated timestamp without time zone NOT NULL DEFAULT Now();

-- Pidxref

ALTER TABLE jtrace.pidxref ADD COLUMN creationdate timestamp without time zone NOT NULL DEFAULT Now();        
ALTER TABLE jtrace.pidxref ADD COLUMN lastupdated timestamp without time zone NOT NULL DEFAULT Now();
ALTER TABLE jtrace.pidxref ADD CONSTRAINT pidxref_compound UNIQUE (sendingfacility, sendingextract, localid);

-- Workitem

ALTER TABLE jtrace.workitem ADD COLUMN creationdate timestamp without time zone NOT NULL DEFAULT Now();
ALTER TABLE jtrace.workitem
ALTER COLUMN lastupdated
SET DEFAULT Now();