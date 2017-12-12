delete from jtrace.audit;
delete from jtrace.linkrecord;
delete from jtrace.masterrecord;
delete from jtrace.person;
delete from jtrace.workitem;
delete from jtrace.tracerequest;
delete from jtrace.traceresponse;
delete from jtrace.traceresponseline;
ALTER SEQUENCE jtrace.ukrdc_id RESTART WITH 100000000;