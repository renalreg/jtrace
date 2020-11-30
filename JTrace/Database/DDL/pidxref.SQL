CREATE TABLE jtrace.pidxref
(
  id serial NOT NULL,
  pid character varying(10) NOT NULL,
  creationdate timestamp without time zone NOT NULL DEFAULT Now(),
  lastupdated timestamp without time zone NOT NULL DEFAULT Now(),
  sendingfacility character varying(7) NOT NULL,
  sendingextract character varying(6) NOT NULL,
  localid character varying(50) NOT NULL
)
WITH (
  OIDS=FALSE
);

ALTER TABLE jtrace.pidxref OWNER TO ukrdc;
ALTER TABLE jtrace.pidxref
    ADD CONSTRAINT pidxref_compound UNIQUE (sendingfacility, sendingextract, localid);
GRANT ALL PRIVILEGES ON TABLE jtrace.pidxref to ukrdc;

GRANT ALL PRIVILEGES ON SEQUENCE jtrace.pidxref_id_seq to ukrdc;