package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

/**
 * Many units may send in data for the same patient. This tests that the links are correctly created
 * @author Nick
 *
 */
public class UKRDCIndexManagerMultiUnitLinkSystemTest {
	
	private Date d1 = getDate("1962-08-31");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");

		clear("MUST100001", "MUST1");
		clear("MUST200001", "MUST2");
		clear("MUST300001", "MUST3");

	}

	@Test
	public void testThreeUnits() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		String orig1 = "MUST1";
		String orig2 = "MUST2";
		String orig3 = "MUST3";
		String nhsNo = "MUST900001";
		String local1 = "MUST100001";
		String local2 = "MUST200001";
		String local3 = "MUST300001";
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(local1).setLocalIdType("MR").setOriginator(orig1);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,nhsNo));
		UKRDCIndexManagerResponse resp = im.store(p1);
		NationalIdentity natId1 = resp.getNationalIdentity();
		// VERIFY
		assert(natId1!=null);
		assert(natId1.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId1.getId().startsWith("10")); // Allocated numbers will start with 10 whereas numbers sent in from test stub begin RR 
		assert(natId1.getId().length()==9);      // Allocated numbers are 9 characters long 
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(natId1.getId(), NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T1-2 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(local2).setLocalIdType("MR").setOriginator(orig2);
		p2.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,nhsNo));
		UKRDCIndexManagerResponse resp2 = im.store(p2);
		NationalIdentity natId2 = resp2.getNationalIdentity();
		// VERIFY 
		assert(natId2!=null);
		assert(natId2.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId2.getId().equals(natId1.getId()));
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(natId2.getId(), NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T1-3 - Third record for the same NHS Number from a different trust
		Person p3 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId(local3).setLocalIdType("MR").setOriginator(orig3);
		p3.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,nhsNo));
		UKRDCIndexManagerResponse resp3 = im.store(p3);
		NationalIdentity natId3 = resp3.getNationalIdentity();
		// VERIFY 
		assert(natId3!=null);
		assert(natId3.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId3.getId().equals(natId1.getId()));
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(natId3.getId(), NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

	}

	private static java.util.Date getDate(String sDate){
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}
	
	public static void clear(String localId, String originator)  throws MpiException {
		
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
}
