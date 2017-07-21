-- Table: jtrace.person

-- DROP TABLE jtrace.person;

CREATE TABLE jtrace.person
(
  masterid SERIAL UNIQUE,
  localid character(10) NOT NULL,
  localidtype character(5) NOT NULL,
  localidoriginator character varying(50) NOT NULL,
  dateofbirth date NOT NULL,
  gender character(2) NOT NULL,
  dateofdeath date,
  givenname character varying(50),
  surname character varying(50),
  prevsurname character varying(50),
  othergivennames character varying(50),
  title character varying(20),
  postcode character(10),
  street character varying(50),
  stdsurname character(4),
  stdprevsurname character(4),
  stdgivenname character(4),
  stdpostcode character(8)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE jtrace.person
  OWNER TO postgres;

