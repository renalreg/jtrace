-- Table: jtrace.audit

DROP TABLE IF EXISTS jtrace.audit;

CREATE TABLE jtrace.audit
(
    id SERIAL UNIQUE,
    personid integer NOT NULL,
    masterid integer NOT NULL,
    type integer NOT NULL,
    description character varying(100) NOT NULL,
    lastupdated timestamp NOT NULL,
    updatedBy character varying(20) ,
    CONSTRAINT audit_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.audit
    OWNER to postgres;