package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerUpdateRecordSystemTest {

	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerUpdateRecordSystemTest.class);
	
	private Date d1 = getDate("1970-01-29");
	private Date d2 = getDate("1970-01-28");

	private Date d3 = getDate("1980-06-01");
	private Date d4 = getDate("1980-06-06");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		
		//  Revised
		clear( "UPDT10001", "UPDT1");
		MasterRecordDAO.deleteByNationalId("UPDT2000R1",NationalIdentity.UKRR_TYPE);
		clear( "UPDT20001", "UPDT2");
		MasterRecordDAO.deleteByNationalId("UPDT3000R1",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT3000R2",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT3000R3",NationalIdentity.UKRR_TYPE);
		clear( "UPDT30001", "UPDT3");
		clear( "UPDT3A001", "UPDT3A");
		MasterRecordDAO.deleteByNationalId("UPDT4000R1",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT4000N1",NationalIdentity.NHS_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT4000C1",NationalIdentity.CHI_TYPE);
		clear( "UPDT40001", "UPDT4");

	}

	@Test
	public void test1SimpleUpdateNoNationalIds() throws MpiException {
		
		logger.debug("************* test1SimpleUpdateNoNationalIds ****************");

		String originator = "UPDT1";
		String idBase = originator+"000";
		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Setup a person with no national ids
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ANT").setGivenName("ADAM").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		im.createOrUpdate(p1);
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);

		logger.debug("************* test1SimpleUpdateNoNationalIds ********UT1-1********");
		// UT1-1 - Simple Update - change surname - still no national ids
		p1.setSurname("ANT2").setPostcode("CH1 6LB").setStreet("Townfield Lane");
		im.createOrUpdate(p1);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		assert(person.getSurname().equals("ANT2"));
		assert(person.getPrevSurname().equals("ANT"));

	}

	@Test
	public void test2UpdateAddingUKRDCId() throws MpiException {

		logger.debug("************* test2UpdateAddingUKRDCId ****************");

		String originator = "UPDT2";
		String idBase = originator+"000";
		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with no national ids
		Person p1 = new Person().setDateOfBirth(d1).setSurname("BOLD").setGivenName("BRENDA").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		im.createOrUpdate(p1);
		
		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-1********");
		// UT2-1 - Add a UKRDC id
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2********");
		// UT2-2 - No UKRDC on inbound, but already on record - just leave it there - this may be a common occurrence
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2A********");
		// UT2-2 - UKRDC on inbound matching that on on record 
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-3********");
		// UT2-3 - As 2 but change demographics - later effective date (not set, so defaulted) should cause master update
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDA");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getEffectiveDate().compareTo(p1.getEffectiveDate())==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-4********");
		// UT2-4 - As 2 but change demographics - earlier effective date - no update of Master. Match still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		//Chcek that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-5********");
		// UT2-5 - As 4 change demographics - earlier effective date - no update of Master. But no longer verifies so work item raised and master marked for investigation
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setDateOfBirth(d4);
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		//Chcek that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED));

	}
	
	@Test
	public void test3UpdateChangingUKRDCId() throws MpiException {

		logger.debug("************* test3UpdateChangingUKRDCId ****************");

		String originator = "UPDT3";
		String originator2 = "UPDT3A";
		String idBase = originator+"000";
		String idBase2 = originator2+"00";
		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with a UKRDC
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		
		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-1********");
		// UT3-1 - Change the UKRDC ID. Should delete the link to the previous and the original master whilst creating new links
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R2");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master==null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId(idBase+"R2",NationalIdentity.UKRR_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-2********");
		// P2 - Setup another person linked to the same UKRDC
		Person p2 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R2");
		im.createOrUpdate(p2);

		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-2********");
		// UT3-2 - Change the UKRDC ID. Should delete the link to the previous. Original master stays in place though because p2 is linked to it
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R3");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R2",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		master2 = MasterRecordDAO.findByNationalId(idBase+"R3",NationalIdentity.UKRR_TYPE);
		assert(master2!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
	}
	
	@Test
	public void test4AddingOtherNationalId() throws MpiException {

		logger.debug("************* test4AddingOtherNationalId ****************");

		String originator = "UPDT4";
		String idBase = originator+"000";

		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with a UKRDC
		logger.debug("************* test4AddingOtherNationalId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("DERBY").setGivenName("DORIS").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-1********");
		// UT4-1 - Add NHS Number and CHI Number
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1"));
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		MasterRecord masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		MasterRecord masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi!=null);

		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterChi.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-2********");
		// UT4-2 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,idBase+"H1"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		MasterRecord masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test4AddingOtherNationalId ********UT4-3********");
		// UT4-3 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBY");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);		

		logger.debug("************* test4AddingOtherNationalId ********UT4-4********");
		// UT4-4 - Update the surname - but set an old effective date. Master records should not be updated. Work Items not created as data still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBEY");
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBEY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);		

	}

	//@Test
	public void testUpdateNatIdOnPreviousNotCurrent() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Setup a person with a national id
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ERIKSON").setGivenName("ERICA").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP5000001").setLocalIdType("MR").setOriginator("PUP5");
		p1.setPrimaryIdType("NHS").setPrimaryId("UHS5000001");
		im.createOrUpdate(p1);
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS5000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		
		// TEST1 - update to remove the nationalId. Will DELINK and delete the master
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000001", "NHS");
		assert(master==null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);

		// P2 - Setup a person with a national id and another linked person. Removing the NationalId from first patient should leave the other patient linked
		Person p2 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP5000002").setLocalIdType("MR").setOriginator("PUP5");
		p2.setPrimaryIdType("NHS").setPrimaryId("UHS5000002");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getPersonId()==p2.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// P3 - Link to National Id
		Person p3 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP5000003").setLocalIdType("MR").setOriginator("PUP5A");
		p3.setPrimaryIdType("NHS").setPrimaryId("UHS5000002");
		im.createOrUpdate(p3);
		
		// TEST2 - update to remove the nationalId. Will DELINK P2 but leave the master linked to P3. Raise a WORK because of the demog match with P3
		p2.setId(0); // reset the id - incoming record won't have this.
		p2.setPrimaryIdType(null).setPrimaryId(null);
		im.createOrUpdate(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		links = LinkRecordDAO.findByMaster(master.getId());
		assert(links.size()==1);
		assert(links.get(0).getPersonId()==p3.getId()); // STILL LINKED TO P3
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // FROM P2 SETUP
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
	}
	
	//@Test
	public void testUpdateNatIdNotOnCurrentOrPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Update with a master id that does not exist and has no demographic match
		// P1 - Set up person with no national id
		Person p1 = new Person().setDateOfBirth(d1).setSurname("DAVIES").setGivenName("DEREK").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP4000001").setLocalIdType("MR").setOriginator("PUP4");
		im.createOrUpdate(p1);
		
		// TEST1 - update no change and no DEMOG matches - no master/links/work created
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// P2 - set up a person with master and same demographics to create a demogs match and WORK ITEM
		Person p2 = new Person().setDateOfBirth(d1).setSurname("DAVIES").setGivenName("DEREK").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP4000002").setLocalIdType("MR").setOriginator("PUP4");
		p2.setPrimaryIdType("NHS").setPrimaryId("UHS4000001");
		im.createOrUpdate(p2);

		// TEST2 - now update p1 again and "find" demog match - work created
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS4000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);

	}

	//@Test
	public void testUpdateNatIdOnCurrentNotPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Update with a master id that does not exist and has no demographic match
		// SETUP person with no national id
		Person p1 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP3000001").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p1);
		
		// TEST1 - add a National Id which doesn't exist. Add a master and a link. No similar demogs so no WORK
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType("NHS").setPrimaryId("UHS3000001");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS3000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// P2 - Update with a master id that does not exist but has a demographic match
		// SETUP person with no national id - no master but raises a WORK due to demographic match
		Person p2 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP3000002").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST2 - add a National Id which doesn't exist. Add a master and a link. Similar demogs exist so another WORK created
		p2.setId(0); // reset the id - incoming record won't have this.
		p2.setPrimaryIdType("NHS").setPrimaryId("UHS3000002");
		im.createOrUpdate(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==4); // TODO: STOP DUPLICATE WORK ITEMS
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(3).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);

		// P3 - Update with a master id that does exist and verifies
		// SETUP person with no national id - no master but raises 3 WORK items due to demographic match with UHS3000001 & UHS3000002 and Algorithmic Match
		Person p3 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP3000003").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);  // TODO - Suppress duplicates
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST3 add a National Id which does exist and verifies. Links to that master. Algorithmic WORK created and Demog Match WORK created to UHS3000001
		p3.setId(0); // reset the id - incoming record won't have this.
		p3.setPrimaryIdType("NHS").setPrimaryId("UHS3000002");
		im.createOrUpdate(p3);
		// VERIFY
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==5);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID); // TODO - Suppress duplicates
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(3).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(4).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);  
		
		// P4 - Update with a master id that does exist and does not verifies
		// SETUP person with no national id - no master and no MATCH WORK items, but a ALGO WORK ITEM as demographics are not an exact match
		Person p4 = new Person().setDateOfBirth(d4).setSurname("CLOWN").setGivenName("KAROL").setGender("2");
		p4.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p4.setLocalId("PUP3000004").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p4);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p4.getLocalIdType(), p4.getLocalId(), p4.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST4 - add a National Id which does exist but does not verify. No link but a WORK item created for fail to match and another for the algorithmic match
		p4.setId(0); // reset the id - incoming record won't have this.
		p4.setPrimaryIdType("NHS").setPrimaryId("UHS3000002");
		im.createOrUpdate(p4);
		// VERIFY
		person = PersonDAO.findByLocalId(p4.getLocalIdType(), p4.getLocalId(), p4.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(1).getType()==WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress duplicates
		
		assert(true);
	
	}

	//@Test
	public void testUpdateNatIdOnCurrentAndPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		//SETUP person with NationalId. New Person, New MAster and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setPrimaryIdType("NHS").setPrimaryId("UHS1000001").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP1000001").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p1);
		
		// SETUP another record linked to the NationalId for P1. This should be delinked by later demographic update as only a partial DOB match and given name different. WORK for ALGO match
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setPrimaryIdType("NHS").setPrimaryId("UHS1000001").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP1000002").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// SETUP another record linked to the NationalId for P1. This should NOT be delinked by later demographic update as the full DOB matches the update which will arrive in P1A
		Person p3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("KAREN").setPrimaryIdType("NHS").setPrimaryId("UHS1000001").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP1000003").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p3.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// TEST1 - Update - National Id matches existing record. Demographics have not changed so DQ checks not executed. Simple update to the record.
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P2 still linked
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P3 still linked
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);
		
		// TEST2 - Update - National Id matches existing record. Demographics have changed. Update the record and the master record. SHOULD DELINK PUP1000002 but not PUP1000003
		//                  No WORK as Algorithmic match below threshold
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setDateOfBirth(d2).setGivenName("CAROL");
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==0); 							// P2 no longer linked
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P3 still linked
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);
		
	}
	
	//@Test
	public void testUpdateNatIdChange() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// SETUP Person and Master for update with no other links
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ANDERSON").setGivenName("ALAN").setPrimaryIdType("NHS").setPrimaryId("UHS2000001").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP2000001").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p1);
		// VERIFY SETUP
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS2000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);

		// SETUP Person and Master for update which will have other links - master and other link should remain when this patient changes NHS Number
		Person p2 = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setPrimaryIdType("NHS").setPrimaryId("UHS2000002").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP2000002").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==0);

		// SETUP - 2nd Person to link to P1
		Person p3 = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setPrimaryIdType("NHS").setPrimaryId("UHS2000002").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP2000003").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p3.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// TEST1 - Change NHS Number. Should delink this person from the existing master and delete that master. Then add and link to a new master
		p1.setPrimaryIdType("NHS").setPrimaryId("UHS2000003");
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS2000001", "NHS");
		assert(master==null);
		master = MasterRecordDAO.findByNationalId("UHS2000003", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);

		// TEST2 - Change NHS Number. Should delink this person from the existing master but not delete that master because of PUP2000003. Then add and link to a new master. Raise WORK due to same demographics
		p2.setPrimaryIdType("NHS").setPrimaryId("UHS2000004");
		im.createOrUpdate(p2);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		master = MasterRecordDAO.findByNationalId("UHS2000004", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID); 
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record
		
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
