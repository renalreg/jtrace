-- Table: jtrace.pidxref

DROP TABLE IF EXISTS jtrace.pidxref;

CREATE TABLE jtrace.pidxref
(
  id serial NOT NULL,
  pid character varying(10) NOT NULL,
  sendingfacility character varying(7) NOT NULL,
  sendingextract character varying(6) NOT NULL,
  localpatientid character varying(17) NOT NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.pidxref
  OWNER TO postgres;