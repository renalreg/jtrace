-- Table: jtrace.linkrecord

DROP TABLE IF EXISTS jtrace.linkrecord;

CREATE TABLE jtrace.linkrecord
(
  id serial NOT NULL,
  personid integer NOT NULL,
  masterid integer NOT NULL,
  linktype integer NOT NULL,
  linkcode integer NOT NULL,
  updatedBy character varying(20) ,
  lastupdated timestamp without time zone NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.linkrecord
  OWNER TO postgres;