-- Table: jtrace.masterreference

-- DROP TABLE jtrace.masterreference;

CREATE TABLE jtrace.masterreference
(
  masterid integer NOT NULL,
  localid character(10) NOT NULL,
  localidtype character varying(50) NOT NULL,
  originator character varying(50) NOT NULL,
  supercededby integer NOT NULL,
  CONSTRAINT masterreference_pkey PRIMARY KEY (localid, localidtype, originator, supercededby)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.masterreference
  OWNER TO postgres;
