-- JTrace version 1.4.5
-- Convert fixed-width character fields to VARCHAR and remove padding.

-- Preserve existing wider/unlimited definitions while removing CHAR padding.
-- USING trims during conversion; UPDATE on a CHAR column would just pad it again.
DO $$
DECLARE
    field record;
    existing_width integer;
    target_type text;
BEGIN
    FOR field IN SELECT * FROM (VALUES
        ('masterrecord', 'nationalidtype', 50),
        ('tracerequest', 'traceid', 36),
        ('tracerequest', 'tracetype', 10),
        ('tracerequest', 'nameswap', 1),
        ('tracerequest', 'localid', 10),
        ('tracerequest', 'localidtype', 5),
        ('tracerequest', 'gender', 2),
        ('tracerequest', 'postcode', 10),
        ('traceresponse', 'traceid', 36),
        ('traceresponse', 'tracestarttime', 23),
        ('traceresponse', 'traceendtime', 23),
        ('traceresponse', 'status', 10),
        ('traceresponseline', 'traceid', 36),
        ('traceresponseline', 'gender', 2),
        ('traceresponseline', 'postcode', 10)
    ) AS fields(table_name, column_name, minimum_width)
    LOOP
        SELECT character_maximum_length INTO STRICT existing_width
          FROM information_schema.columns
         WHERE table_schema = 'jtrace'
           AND table_name = field.table_name AND column_name = field.column_name;
        target_type := CASE WHEN existing_width IS NULL THEN 'character varying'
            ELSE format('character varying(%s)', greatest(existing_width, field.minimum_width)) END;
        EXECUTE format('ALTER TABLE jtrace.%I ALTER COLUMN %I TYPE %s USING rtrim(%I::text)',
            field.table_name, field.column_name, target_type, field.column_name);
    END LOOP;
END;
$$;
