-- Support national identifier organizations such as UKRR_UID (8 characters).
-- Apply even if earlier versions of the migrations have already been run.
-- Preserve wider/unlimited columns and remove trailing padding without truncation.

DO $$
DECLARE
    field record;
    existing_width integer;
    target_type text;
BEGIN
    FOR field IN SELECT * FROM (VALUES
        ('masterrecord', 'nationalidtype'),
        ('person', 'nationalidtype'),
        ('audit', 'mainnationalidtype')
    ) AS fields(table_name, column_name)
    LOOP
        SELECT character_maximum_length INTO STRICT existing_width
          FROM information_schema.columns
         WHERE table_schema = 'jtrace'
           AND table_name = field.table_name AND column_name = field.column_name;
        target_type := CASE WHEN existing_width IS NULL THEN 'character varying'
            ELSE format('character varying(%s)', greatest(existing_width, 50)) END;
        EXECUTE format('ALTER TABLE jtrace.%I ALTER COLUMN %I TYPE %s USING rtrim(%I::text)',
            field.table_name, field.column_name, target_type, field.column_name);
    END LOOP;
END;
$$;
