-- Table: jtrace.linkrecord

DROP TABLE IF EXISTS jtrace.linkrecord;

CREATE TABLE jtrace.linkrecord
(
  id serial NOT NULL,
  personid integer NOT NULL,
  masterid integer NOT NULL,
  linktype integer NOT NULL,
  linkcode integer NOT NULL,
  linkdesc character varying(200) ,
  updatedBy character varying(20) ,
  lastupdated timestamp without time zone NOT NULL default now(),
  creationdate timestamp without time zone NOT NULL default now()
)
WITH (
  OIDS=FALSE
);

ALTER TABLE jtrace.linkrecord OWNER to ukrdc;
    
GRANT ALL PRIVILEGES ON TABLE jtrace.linkrecord to ukrdc;