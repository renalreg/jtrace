-- Table: jtrace.masterrecord

DROP TABLE IF EXISTS jtrace.masterrecord;

CREATE TABLE jtrace.masterrecord
(
    id SERIAL UNIQUE,
    lastupdated timestamp without time zone NOT NULL,
    dateofbirth date NOT NULL,
    gender character varying(5) COLLATE pg_catalog."default",
    givenname character varying(50) COLLATE pg_catalog."default",
    surname character varying(50) COLLATE pg_catalog."default",
    nationalid character(10) COLLATE pg_catalog."default" NOT NULL,
    nationalidtype character(5) COLLATE pg_catalog."default" NOT NULL,
    status int NOT NULL,
    effectivedate timestamp without time zone NOT NULL,
    CONSTRAINT masterrecord_nationalid_key UNIQUE (nationalid, nationalidtype)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.masterrecord
    OWNER to postgres;