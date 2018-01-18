package com.agiloak.mpi.index;

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

	protected static void clear(String localId, String originator) throws MpiException {
		
		Person person = PersonDAO.findByLocalId("MR", localId, originator);
		if (person != null) {
			List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
			for (LinkRecord link : links) {
				MasterRecordDAO.delete(link.getMasterId());
			}
			LinkRecordDAO.deleteByPerson(person.getId());
			WorkItemDAO.deleteByPerson(person.getId());
			PersonDAO.delete(person);
			String traceId = TraceDAO.getTraceId(localId, "MR", originator, "AUTO");
			if (traceId != null) {
				TraceDAO.clearByTraceId(traceId);
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