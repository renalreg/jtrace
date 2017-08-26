package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerNewRecordSystemTest {
	
	private final static String UKRDC_TYPE = "UKRDC";
	private final static String NHS_TYPE = "NHS";

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1961-08-30");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {

		MasterRecordDAO.deleteByNationalId("RR1000001",UKRDC_TYPE);
		clear("NSYS100001", "NSYS1");
		clear("NSYS100002", "NSYS1");
		clear("NSYS100003", "NSYS1");
		
		MasterRecordDAO.deleteByNationalId("RR2000001",UKRDC_TYPE);
		MasterRecordDAO.deleteByNationalId("NHS0200001",NHS_TYPE);
		clear("NSYS200001", "NSYS2");
		clear("NSYS200002", "NSYS2");
		clear("NSYS200003", "NSYS2");
		
		MasterRecordDAO.deleteByNationalId("RR3000001",UKRDC_TYPE);
		MasterRecordDAO.deleteByNationalId("NHS0300001",NHS_TYPE);
		clear("NSYS300001", "NSYS3");
		clear("NSYS300002", "NSYS3");
		clear("NSYS300003", "NSYS3");
		
		MasterRecordDAO.deleteByNationalId("RR4000001",UKRDC_TYPE);
		clear("NSYS400001", "NSYS4");
		clear("NSYS400002", "NSYS4");
		clear("NSYS400003", "NSYS4");

		MasterRecordDAO.deleteByNationalId("RR5000001",UKRDC_TYPE);
		clear("NSYS500001", "NSYS5");
		clear("NSYS500002", "NSYS5");
		clear("NSYS500003", "NSYS5");

	}

	@Test
	public void testNewWithPrimaryId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR1000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100001").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("RR1000001", UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T1-2 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR1000001").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS100002").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p2);
		// VERIFY 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("RR1000001", UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T1-3 - New + NationalId + NationalId exists + No match (DOB 2 part and given name mismatch). New Person & Link but mark Master for INVESTIGATION & work item about mismatched Master
		Person p3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("MATTY").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR1000001").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS100003").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p3);
		// VERIFY 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("RR1000001", UKRDC_TYPE);
		assert(master!=null);
		assert(master.getStatus()==MasterRecord.INVESTIGATE);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED);

	}

	@Test
	public void testNewWithPrimaryIdEffDate() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// T4-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR4000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS400001").setLocalIdType("MR").setOriginator("NSYS4");
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("RR4000001", UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-01"))==0);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T4-2 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master. Effective date is later than previous update so master is updated 
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS2").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR4000001").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS400002").setLocalIdType("MR").setOriginator("NSYS4");
		p2.setEffectiveDate(getDate("2017-08-02"));
		im.createOrUpdate(p2);
		// VERIFY 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("RR4000001", UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-02"))==0);
		assert(master.getGivenName().equals(p2.getGivenName()));
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T4-3 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master. Effective date is earlier than previous update so no master is updated 
		Person p3 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS3").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR4000001").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS400003").setLocalIdType("MR").setOriginator("NSYS4");
		p3.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p3);
		// VERIFY 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("RR4000001", UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-02"))==0);
		assert(master.getGivenName().equals(p2.getGivenName()));
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

	}

	@Test
	public void testNewWithPrimaryIdEffDate2() throws MpiException {

		String orig = "NSYS5";
		String rrid = "RR5000001";
		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// T5-1 Setup step. NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(UKRDC_TYPE).setPrimaryId(rrid).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS500001").setLocalIdType("MR").setOriginator(orig);
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(rrid, UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-01"))==0);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T5-2 - New + NationalId + NationalId exists + Not verified. New Person & Link to existing Master. Effective date is later than previous update so master is updated 
		// WorkItem created for mismatch. Master marked for investigation
		Person p2 = new Person().setDateOfBirth(d3).setSurname("JONES").setGivenName("NICHOLAS2").setPrimaryIdType(UKRDC_TYPE).setPrimaryId(rrid).setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS500002").setLocalIdType("MR").setOriginator(orig);
		p2.setEffectiveDate(getDate("2017-08-02"));
		im.createOrUpdate(p2);
		// VERIFY 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(rrid, UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-02"))==0);
		assert(master.getGivenName().equals(p2.getGivenName()));
		assert(master.getStatus()==MasterRecord.INVESTIGATE);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		
		// T5-3 - New + NationalId + NationalId exists + Not verified. New Person & Link to existing Master. Effective date is earlier than previous update so no master update 
		// WorkItem created for mismatch. Master marked for investigation
		Person p3 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS3").setPrimaryIdType(UKRDC_TYPE).setPrimaryId(rrid).setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS500003").setLocalIdType("MR").setOriginator(orig);
		p3.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p3);
		// VERIFY 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(rrid, UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-02"))==0);
		assert(master.getGivenName().equals(p2.getGivenName()));
		assert(master.getStatus()==MasterRecord.INVESTIGATE);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);

	}	
	@Test
	public void testNewWithNoPrimaryId1() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// Setup - Person with NationalId for match to TEST2. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR2000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS200001").setLocalIdType("MR").setOriginator("NSYS2");
		p1.addNationalId(new NationalIdentity("NHS","NHS0200001"));
		im.createOrUpdate(p1);
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master1 = MasterRecordDAO.findByNationalId("RR2000001", UKRDC_TYPE);
		assert(master1!=null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId("NHS0200001", NHS_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==master1.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T2-1 - New + No NationalId No Matches to any master. New Person only
		Person p2 = new Person().setDateOfBirth(d1).setSurname("LORIMER").setGivenName("PETER").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS200002").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p2);
		// VERIFY 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T2-2 - New + No NationalId + Matches to existing NHS Number. New Person and work
		Person p3 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS200003").setLocalIdType("MR").setOriginator("NSYS2");
		p3.addNationalId(new NationalIdentity("NHS","NHS0200001"));
		im.createOrUpdate(p3);
		// VERIFY 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); 
		assert(links.get(1).getMasterId()==master1.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
	}	

	@Test
	public void testNewWithNoPrimaryId2() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// Setup - Person with UKRDC
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS300001").setLocalIdType("MR").setOriginator("NSYS3");
		p1.setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR3000001");
		p1.setEffectiveDate(getDate("2017-08-02"));
		im.createOrUpdate(p1);
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord masterRR = MasterRecordDAO.findByNationalId("RR3000001", UKRDC_TYPE);
		assert(masterRR!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==masterRR.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// Setup - Person with UKRDC Number and NHS Number. DOB only a 2-point match, but good enough to match. Old Effective date stops UKRDC Master being updated so allowing step 3 to be unverified on DOB 
		Person p2 = new Person().setDateOfBirth(d2).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS300002").setLocalIdType("MR").setOriginator("NSYS3");
		p2.setPrimaryIdType(UKRDC_TYPE).setPrimaryId("RR3000001");
		p2.addNationalId(new NationalIdentity(NHS_TYPE,"NHS0300001"));
		p2.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p2);
		// VERIFY 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		MasterRecord masterNHS = MasterRecordDAO.findByNationalId("NHS0300001", NHS_TYPE);
		assert(masterNHS!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==masterNHS.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==masterRR.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T3-1 - New + No UKRDC Number but an NHS Number which matches to an existing NHS Number which could corroborate the UKRDC - but UKRDC:Person don't match (1 part DOB only)
		Person p3 = new Person().setDateOfBirth(d3).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS300003").setLocalIdType("MR").setOriginator("NSYS3");
		p3.addNationalId(new NationalIdentity(NHS_TYPE,"NHS0300001"));
		im.createOrUpdate(p3);
		// VERIFY 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1); // Linked to the NHS Number but not to the UKRDC
		assert(links.get(0).getMasterId()==masterNHS.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		
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
