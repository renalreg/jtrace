-- JTrace version 1.4.3
-- Allow incoming PatientNumber identifiers to contain up to 50 characters.

DO $$
DECLARE
    field record;
    existing_width integer;
BEGIN
    FOR field IN SELECT * FROM (VALUES
        ('masterrecord', 'nationalid'),
        ('person', 'nationalid'),
        ('audit', 'mainnationalid')
    ) AS fields(table_name, column_name)
    LOOP
        SELECT character_maximum_length INTO STRICT existing_width
          FROM information_schema.columns
         WHERE table_schema = 'jtrace'
           AND table_name = field.table_name AND column_name = field.column_name;
        IF existing_width < 50 THEN
            EXECUTE format('ALTER TABLE jtrace.%I ALTER COLUMN %I TYPE character varying(50)',
                field.table_name, field.column_name);
        END IF;
    END LOOP;
END;
$$;
