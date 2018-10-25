-- Table: jtrace.person
CREATE TABLE jtrace.person
(
  id SERIAL UNIQUE,
  originator character varying(50) NOT NULL,
  localid character(17) NOT NULL,
  localidtype character(10) NOT NULL,
  nationalid character(10),
  nationalidtype character(5),
  dateofbirth date NOT NULL,
  gender character(2) NOT NULL,
  dateofdeath date,
  givenname character varying(60),
  surname character varying(60),
  prevsurname character varying(60),
  othergivennames character varying(60),
  title character varying(20),
  postcode character(10),
  street character varying(220),
  stdsurname character(4),
  stdprevsurname character(4),
  stdgivenname character(4),
  stdpostcode character(8),
  skipduplicatecheck boolean
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.person
  OWNER TO ukrdc;

GRANT ALL PRIVILEGES ON TABLE jtrace.person to ukrdc;

