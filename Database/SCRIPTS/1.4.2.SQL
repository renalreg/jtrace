-- JTrace version 1.4.2
-- 28/11/2020

-- These are minimum widths, not limits to impose on already-wider deployments.
DO $$
DECLARE
    field record;
    existing_width integer;
BEGIN
    FOR field IN SELECT * FROM (VALUES
        ('masterrecord', 'nationalid', 50),
        ('pidxref', 'localid', 50),
        ('person', 'nationalid', 50),
        ('person', 'localid', 50),
        ('audit', 'updatedby', 320),
        ('linkrecord', 'updatedby', 320),
        ('workitem', 'updatedby', 320)
    ) AS fields(table_name, column_name, minimum_width)
    LOOP
        SELECT character_maximum_length INTO STRICT existing_width
          FROM information_schema.columns
         WHERE table_schema = 'jtrace'
           AND table_name = field.table_name AND column_name = field.column_name;
        -- NULL means an unlimited VARCHAR/TEXT column; leave its capacity unchanged.
        IF existing_width < field.minimum_width THEN
            EXECUTE format('ALTER TABLE jtrace.%I ALTER COLUMN %I TYPE character varying(%s)',
                field.table_name, field.column_name, field.minimum_width);
        END IF;
    END LOOP;
END;
$$;
