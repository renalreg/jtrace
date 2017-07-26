-- Table: jtrace.workitem

DROP TABLE IF EXISTS jtrace.workitem;

CREATE TABLE jtrace.workitem
(
    id SERIAL UNIQUE,
    personid integer NOT NULL,
    type integer NOT NULL,
    description character varying(100) COLLATE pg_catalog."default" NOT NULL,
    status integer NOT NULL,
    lastupdated timestamp NOT NULL,
    CONSTRAINT workitem_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.workitem
    OWNER to postgres;