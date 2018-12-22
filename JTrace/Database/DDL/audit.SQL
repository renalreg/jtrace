DROP TABLE IF EXISTS jtrace.audit;

CREATE TABLE jtrace.audit
(
    id SERIAL UNIQUE,
    personid integer NOT NULL,
    masterid integer NOT NULL,
    type integer NOT NULL,
    description character varying(100) NOT NULL,
    attributes character varying(1024),
    mainnationalid character varying(10),
    mainnationalidtype character varying(5),
    lastupdated timestamp NOT NULL,
    updatedBy character varying(20) ,
    CONSTRAINT audit_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.audit OWNER to ukrdc;
    
GRANT ALL PRIVILEGES ON TABLE jtrace.audit to ukrdc;