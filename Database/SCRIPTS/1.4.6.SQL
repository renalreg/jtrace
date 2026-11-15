-- JTrace version 1.4.6
-- Optional performance index for automatic closure of open type 9 work items.
-- Run outside a transaction: CONCURRENTLY allows work-item writes during the build.

CREATE INDEX CONCURRENTLY IF NOT EXISTS workitem_open_source ON jtrace.workitem
    ((attributes::json ->> 'MRN'), (attributes::json ->> 'SF'), (attributes::json ->> 'SE'))
    WHERE type = 9 AND status = 1;
