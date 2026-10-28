DROP TABLE IF EXISTS jtrace.tracerequest;

CREATE TABLE jtrace.tracerequest
(
  traceid character varying(36) NOT NULL,
  tracetype character varying(10) NOT NULL,
  nameswap character varying(1) NOT NULL,
  localid character varying(10) NOT NULL,
  localidtype character varying(5) NOT NULL,
  originator character varying(50) NOT NULL,
  givenname character varying(50),
  othergivennames character varying(50),
  surname character varying(50),
  gender character varying(2),
  postcode character varying(10),
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

ALTER TABLE jtrace.tracerequest OWNER TO ukrdc;

GRANT ALL PRIVILEGES ON TABLE jtrace.tracerequest to ukrdc;
