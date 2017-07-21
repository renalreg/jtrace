-- Table: jtrace.traceresponse

-- DROP TABLE jtrace.traceresponse;

CREATE TABLE jtrace.traceresponse
(
  traceid character(36) NOT NULL,
  tracestarttime character(23) NOT NULL,
  traceendtime character(23) NOT NULL,
  message character varying(200),
  status character(10) NOT NULL,
  maxweight double precision,
  matchcount integer,
  CONSTRAINT traceresponse_pkey PRIMARY KEY (traceid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.traceresponse
  OWNER TO postgres;

