DROP TABLE IF EXISTS jtrace.person;

CREATE TABLE jtrace.person
(
  id SERIAL UNIQUE,
  originator character varying(50) NOT NULL,
  localid character varying(17) NOT NULL,
  localidtype character varying(10) NOT NULL,
  nationalid character varying(10),
  nationalidtype character varying(5),
  dateofbirth date NOT NULL,
  gender character varying(2) NOT NULL,
  dateofdeath date,
  givenname character varying(60),
  surname character varying(60),
  prevsurname character varying(60),
  othergivennames character varying(60),
  title character varying(20),
  postcode character varying(10),
  street character varying(220),
  stdsurname character varying(4),
  stdprevsurname character varying(4),
  stdgivenname character varying(4),
  stdpostcode character varying(8),
  skipduplicatecheck boolean
)
WITH (
  OIDS=FALSE
);

ALTER TABLE jtrace.person OWNER TO ukrdc;

GRANT ALL PRIVILEGES ON TABLE jtrace.person to ukrdc;

