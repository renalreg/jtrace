-- Table: jtrace.workitem

DROP TABLE IF EXISTS jtrace.workitem;

CREATE TABLE jtrace.workitem
(
    id SERIAL UNIQUE,
    personid integer NOT NULL,
    masterid integer NOT NULL,
    type integer NOT NULL,
    description character varying(100) NOT NULL,
    status integer NOT NULL,
    lastupdated timestamp NOT NULL,
    updatedBy character varying(20) ,
    updatedesc character varying(100),
    CONSTRAINT workitem_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE jtrace.workitem
    OWNER to postgres;