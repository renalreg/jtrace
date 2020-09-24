SET search_path TO "$user", jtrace, extract;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA jtrace TO ukrdc;
GRANT ALL ON SEQUENCE person_id_seq TO ukrdc;
GRANT ALL ON SEQUENCE audit_id_seq TO ukrdc;
GRANT ALL ON SEQUENCE linkrecord_id_seq to ukrdc;
GRANT ALL ON SEQUENCE masterrecord_id_seq to ukrdc;
GRANT ALL ON SEQUENCE person_id_seq to ukrdc;
GRANT ALL ON SEQUENCE ukrdc_id to ukrdc;
GRANT ALL ON SEQUENCE workitem_id_seq to ukrdc;
