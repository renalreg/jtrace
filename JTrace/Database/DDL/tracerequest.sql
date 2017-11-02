-- Table: jtrace.tracerequest

DROP TABLE IF EXISTS jtrace.tracerequest;

CREATE TABLE jtrace.tracerequest
(
  traceid character(36) NOT NULL,
  tracetype character(10) NOT NULL,
  nameswap character(1) NOT NULL,
  localid character(10) NOT NULL,
  localidtype character(5) NOT NULL,
  originator character varying(50) NOT NULL,
  givenname character varying(50),
  othergivennames character varying(50),
  surname character varying(50),
  gender character(2),
  postcode character(10),
  dateofbirthstart date,
  dateofbirthend date,
  street character varying(50),
  longname character varying(100),
  longaddress character varying(100),
  CONSTRAINT tracerequest_pkey PRIMARY KEY (traceid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.tracerequest
  OWNER TO postgres;
