-- Table: jtrace.traceresponse

DROP TABLE IF EXISTS jtrace.traceresponse;

CREATE TABLE jtrace.traceresponse
(
  traceid character varying(36) NOT NULL,
  tracestarttime character varying(23) NOT NULL,
  traceendtime character varying(23) NOT NULL,
  message character varying(200),
  status character varying(10) NOT NULL,
  maxweight double precision,
  matchcount integer,
  CONSTRAINT traceresponse_pkey PRIMARY KEY (traceid)
)
WITH (
  OIDS=FALSE
);

ALTER TABLE jtrace.traceresponse OWNER TO ukrdc;

GRANT ALL PRIVILEGES ON TABLE jtrace.traceresponse to ukrdc;
