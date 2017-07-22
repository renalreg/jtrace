-- Table: jtrace.traceresponseline

DROP TABLE IF EXISTS jtrace.traceresponseline;

CREATE TABLE jtrace.traceresponseline
(
  traceid character(36) NOT NULL,
  masterid integer NOT NULL,
  weight double precision,
  givenname character varying(50),
  othergivennames character varying(50),
  surname character varying(50),
  prevsurname character varying(50),
  gender character(2),
  postcode character(10),
  dateofbirth date,
  street character varying(50),
  longname character varying(100),
  longaddress character varying(100),
  CONSTRAINT traceresponseline_pkey PRIMARY KEY (traceid, masterid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.traceresponseline
  OWNER TO postgres;
