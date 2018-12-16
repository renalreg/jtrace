package com.agiloak.mpi.index;

import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.WorkItemType;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerNewRecordSystemTest extends UKRDCIndexManagerBaseTest {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerNewRecordSystemTest.class);

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1961-08-30");


	@BeforeClass
	public static void setup()  throws MpiException {
		logger.debug("***************START OF SETUP****************");
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");

		clear("NSYS100001", "NSYS1");
		clear("NSYS100002", "NSYS1");
		clear("NSYS100003", "NSYS1");
		
		clear("NSYS200001", "NSYS2A");
		clear("NSYS200002", "NSYS2A");
		clear("NSYS200003", "NSYS2B");
		
		clear("NSYS300001", "NSYS3A");
		clear("NSYS300002", "NSYS3A");
		clear("NSYS300003", "NSYS3B");
		
		clear("NSYS400001", "NSYS4");
		clear("NSYS400002", "NSYS4");
		clear("NSYS400003", "NSYS4");

		clear("NSYS500001", "NSYS5");
		clear("NSYS500002", "NSYS5");
		clear("NSYS500003", "NSYS5");

		clear("NSYS600001", "NSYS6");
		clear("NSYS600002", "NSYS6");

		clear("NSYS700001", "NSYS7");
		clear("NSYS700002", "NSYS7");
		clear("NHS0700001", "NSYS7");
		clear("NHS0700002", "NSYS7");
		logger.debug("***************END OF SETUP****************");
	}

	@Test
	public void testNewWithPrimaryId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "NSYS1";
		String ukrdcId = "RR1000001";
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname(" JONES").setGivenName("NICHOLAS ").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100001").setLocalIdType("MR").setOriginator(orig);
		NationalIdentity natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T1-2 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES ").setGivenName(" NICHOLAS").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS100002").setLocalIdType("MR").setOriginator(orig);
		natId = store(p2);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T1-3 - New + NationalId + NationalId exists + No match (DOB 2 part and given name mismatch). New Person & Link but mark Master for INVESTIGATION & work item about mismatched Master
		Person p3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("MATTY").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS100003").setLocalIdType("MR").setOriginator(orig);
		natId = store(p3);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		assert(master.getStatus()==MasterRecord.INVESTIGATE);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY);

	}

	@Test
	public void testNewWithPrimaryIdEffDate() throws MpiException {

		logger.debug("***************START TEST****************");

		String orig = "NSYS4";
		String ukrdcId = "RR4000001";
		
		// T4-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS400001").setLocalIdType("MR").setOriginator(orig);
		p1.setEffectiveDate(getDate("2017-08-01"));
		NationalIdentity natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-01"))==0);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T4-2 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master. Effective date is later than previous update so master is updated 
		Person p2 = new Person().setDateOfBirth(d1).setSurname("jones").setGivenName("nicholas2").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS400002").setLocalIdType("MR").setOriginator(orig);
		p2.setEffectiveDate(getDate("2017-08-02"));
		natId = store(p2);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-02"))==0);
		assert(master.getGivenName().equals(p2.getGivenName()));
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T4-3 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master. Effective date is earlier than previous update so no master is updated 
		Person p3 = new Person().setDateOfBirth(d1).setSurname(" JonES ").setGivenName(" NicHolAs3 ").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS400003").setLocalIdType("MR").setOriginator(orig);
		p3.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p3);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
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

		logger.debug("***************START TEST****************");

		String orig = "NSYS5";
		String ukrdcId = "RR5000001";
		
		// T5-1 Setup step. NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS500001").setLocalIdType("MR").setOriginator(orig);
		p1.setEffectiveDate(getDate("2017-08-01"));
		NationalIdentity natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		assert(master.getEffectiveDate().compareTo(getDate("2017-08-01"))==0);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T5-2 - New + NationalId + NationalId exists + Not verified. New Person & Link to existing Master. Effective date is later than previous update so master is updated 
		// WorkItem created for mismatch. Master marked for investigation
		Person p2 = new Person().setDateOfBirth(d3).setSurname("JONES").setGivenName("NICHOLAS2").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS500002").setLocalIdType("MR").setOriginator(orig);
		p2.setEffectiveDate(getDate("2017-08-02"));
		natId = store(p2);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
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
		Person p3 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS3").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS500003").setLocalIdType("MR").setOriginator(orig);
		p3.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p3);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
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

		logger.debug("***************START TEST****************");

		String ukrdcId = "RR2000001";
		String orig = "NSYS2A";
		String orig2 = "NSYS2B";
		
		// Setup - Person with NationalId for match to TEST2. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS200001").setLocalIdType("MR").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0200001"));
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master1 = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master1!=null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId("NHS0200001", NationalIdentity.NHS_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==master1.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// T2-1 - New + No NationalId No Matches to any master. New Person and Allocate Master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("LORIMER").setGivenName("PETER").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS200002").setLocalIdType("MR").setOriginator(orig);
		natId = store(p2);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(!natId.getId().equals(ukrdcId));
		assert(natId.getId().startsWith("50")); // Allocated numbers will start with 50 whereas numbers sent in from test stub begin RR 
		assert(natId.getId().length()==9);      // Allocated numbers are 9 characters long 
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		MasterRecord allocatedMr = MasterRecordDAO.get(links.get(0).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		List<Audit> audits = AuditDAO.findByPerson(person.getId());
		assert(audits.size()==1);
		assert(audits.get(0).getType()==Audit.NO_MATCH_ASSIGN_NEW);
		assert(audits.get(0).getMasterId()==allocatedMr.getId());
		
		// T2-2 - New + No NationalId + Matches to existing NHS Number. New Person and work
		Person p3 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS200003").setLocalIdType("MR").setOriginator(orig2);
		p3.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0200001"));
		natId = store(p3);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
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

		logger.debug("***************START TEST****************");

		String ukrdcId = "RR3000001";
		String orig = "NSYS3A";
		String orig2 = "NSYS3B";
		
		// Setup - Person with UKRDC
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS300001").setLocalIdType("MR").setOriginator(orig);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId);
		p1.setEffectiveDate(getDate("2017-08-02"));
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord masterRR = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(masterRR!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==masterRR.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// Setup - Person with UKRDC Number and NHS Number. DOB only a 2-point match, but good enough to match. Old Effective date stops UKRDC Master being updated so allowing step 3 to be unverified on DOB 
		Person p2 = new Person().setDateOfBirth(d2).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS300002").setLocalIdType("MR").setOriginator(orig);
		p2.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId);
		p2.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0300001"));
		p2.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p2);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		MasterRecord masterNHS = MasterRecordDAO.findByNationalId("NHS0300001", NationalIdentity.NHS_TYPE);
		assert(masterNHS!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==masterNHS.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==masterRR.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T3-1 - New + No UKRDC Number but an NHS Number which matches to an existing NHS Number which could corroborate the UKRDC - but UKRDC:Person don't match (1 part DOB only). Will Allocate
		Person p3 = new Person().setDateOfBirth(d3).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NSYS300003").setLocalIdType("MR").setOriginator(orig2);
		p3.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0300001"));
		natId = store(p3);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(!natId.getId().equals(ukrdcId));
		assert(natId.getId().startsWith("50")); // Allocated numbers will start with 50 whereas numbers sent in from test stub begin RR 
		assert(natId.getId().length()==9);      // Allocated numbers are 9 characters long 
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2); // Linked to the NHS Number and a newly allocated UKRDC number
		assert(links.get(0).getMasterId()==masterNHS.getId());
		MasterRecord allocatedMr = MasterRecordDAO.get(links.get(1).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		List<Audit> audits = AuditDAO.findByPerson(person.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			if (audit.getType()==Audit.NO_MATCH_ASSIGN_NEW) {
				assert(audit.getMasterId()==allocatedMr.getId());
			} else if (audit.getType()==Audit.WORK_ITEM_CREATED) {
				// OK
			} else {
				// shouldn't be any other types
				assert(false);
			}
		}
		
	}	

	@Test
	public void testNewChangeMRN() throws MpiException {

		logger.debug("***************START TEST****************");

		String ukrdcId = "RR6000001";
		String orig = "NSYS6";
		
		// Setup - Person with NationalId for match to TEST2. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS600001").setLocalIdType("MR").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0600001"));
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master1 = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master1!=null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId("NHS0600001", NationalIdentity.NHS_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==master1.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// T2-2 - Change MRN
		p1.setLocalId("NSYS600002");
		natId = store(p1);
		// VERIFY 
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); 
		assert(links.get(1).getMasterId()==master1.getId()); 
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItemType.TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR);
		MasterRecord mr = MasterRecordDAO.get(master2.getId());
		assert(mr.getStatus()==MasterRecord.INVESTIGATE);
		
	}	

	@Test
	public void testNewSetSkip() throws MpiException {

		logger.debug("***************START TEST****************");

		String ukrdcId = "RR7000001";
		String orig = "NSYS7";
		
		// Setup - Person with NationalId for match to TEST2. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS700001").setLocalIdType("MR").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0700001"));
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(!person.isSkipDuplicateCheck());
		MasterRecord master1 = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master1!=null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId("NHS0700001", NationalIdentity.NHS_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==2);
		assert(links.get(0).getMasterId()==master2.getId()); // NHS Numbers linked before UKRDC in current process
		assert(links.get(1).getMasterId()==master1.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// Test 1 - patient from same org with the same NHS Number - should raise a warning
		Person p2 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS700002").setLocalIdType("MR").setOriginator(orig);
		p2.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0700001"));
		NationalIdentity natId2 = store(p2);
		// VERIFY UPDATES
		assert(natId2!=null);
		assert(natId2.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId2.getId().equals(ukrdcId));
		items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItemType.TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR);

		// Test 2 - patient from same org with the same NHS Number, but this is a fake MRN using NHS Number and flagged as such - Warning should be suppressed
		Person p3 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("NHS0700001").setLocalIdType("MR").setOriginator(orig);
		p3.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0700001"));
		p3.setSkipDuplicateCheck(true);
		NationalIdentity natId3 = store(p3);
		// VERIFY UPDATES
		assert(natId3!=null);
		assert(natId3.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId3.getId().equals(ukrdcId));
		items = WorkItemDAO.findByPerson(p3.getId());
		assert(items.size()==0);

		// Test 3 - patient from same org with the same NHS Number, but this is a fake MRN using NHS Number but not flagged as such - Warning should be created
		Person p4 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p4.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p4.setLocalId("NHS0700002").setLocalIdType("MR").setOriginator(orig);
		p4.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0700001"));
		NationalIdentity natId4 = store(p4);
		// VERIFY UPDATES
		assert(natId4!=null);
		assert(natId4.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId4.getId().equals(ukrdcId));
		items = WorkItemDAO.findByPerson(p4.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItemType.TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR);
	}	
	
	@Test
	public void testNewLongLocalId() throws MpiException {

		logger.debug("***************START TEST****************");

		String orig = "NSYS8";
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS1000011234567").setLocalIdType("MR").setOriginator(orig);
		NationalIdentity natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(natId.getId(), NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
	}	
}
