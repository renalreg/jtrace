package com.agiloak.mpi.index;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerBaseTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	public static Connection conn = null;

	protected static java.util.Date getDate(String sDate) {
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}

//	// convenience for legacy tests - should probably be updated over time
//	protected static void clear(String localId, String originator) throws MpiException {
//		
//		clear(conn, "MR", localId, originator);
//	
//	}

	protected static void clear(Connection conn, String type, String localId, String originator) throws MpiException {
		
		Person person = PersonDAO.findByLocalId(conn, type, localId, originator);
		if (person != null) {
			List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
			for (LinkRecord link : links) {
				MasterRecordDAO.delete(conn, link.getMasterId());
			}
			LinkRecordDAO.deleteByPerson(conn, person.getId());
			WorkItemDAO.deleteByPerson(conn, person.getId());
			PersonDAO.delete(conn, person);
			String traceId = TraceDAO.getTraceId(conn, localId, "MR", originator, "AUTO");
			if (traceId != null) {
				TraceDAO.clearByTraceId(conn, traceId);
			}
		}
	
	}
	public UKRDCIndexManagerBaseTest() {
		super();
	}

	protected NationalIdentity store(Person person) {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.store(person);
		NationalIdentity natId = resp.getNationalIdentity();
		return natId;
	}

}