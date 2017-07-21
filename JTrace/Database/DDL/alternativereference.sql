-- Table: jtrace.alternativereference

-- DROP TABLE jtrace.alternativereference;

CREATE TABLE jtrace.alternativereference
(
  arid serial NOT NULL,
  masterid integer NOT NULL,
  altid character(10) NOT NULL,
  altidtype character varying(50) NOT NULL,
  originator character varying(50) NOT NULL,
  supercededby integer NOT NULL,
  CONSTRAINT altref_pkey PRIMARY KEY (arid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.alternativereference
  OWNER TO postgres;