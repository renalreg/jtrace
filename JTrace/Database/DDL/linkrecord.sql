-- Table: jtrace.linkrecord

DROP TABLE IF EXISTS jtrace.linkrecord;

CREATE TABLE jtrace.linkrecord
(
  id serial NOT NULL,
  personid integer NOT NULL,
  masterid integer NOT NULL,
  lastupdated timestamp without time zone NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.linkrecord
  OWNER TO postgres;