-- Table: jtrace.masterrecord

DROP TABLE jtrace.masterrecord;

CREATE TABLE jtrace.masterrecord
(
    id SERIAL UNIQUE,
    lastupdated timestamp without time zone NOT NULL,
    dateofbirth timestamp without time zone NOT NULL,
    gender character varying(5) COLLATE pg_catalog."default",
    givenname character varying(50) COLLATE pg_catalog."default",
    surname character varying(50) COLLATE pg_catalog."default",
    nationalid character(10) NOT NULL UNIQUE
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.masterrecord
    OWNER to postgres;