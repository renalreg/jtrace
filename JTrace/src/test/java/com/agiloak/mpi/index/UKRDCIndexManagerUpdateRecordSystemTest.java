package com.agiloak.mpi.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.WorkItemType;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerUpdateRecordSystemTest extends UKRDCIndexManagerBaseTest {

	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerUpdateRecordSystemTest.class);
	
	private Date d1 = getDate("1970-01-29");
	private Date d4 = getDate("1980-06-06");

	@BeforeClass
	public static void setup()  throws MpiException {
		
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();

		//  Revised
		clear(conn, "MR", "UPDT10001", "UPDT1");
		clear(conn, "MR", "UPDT20001", "UPDT2");
		clear(conn, "MR", "UPDT30001", "UPDT3");
		clear(conn, "MR", "UPDT3A001", "UPDT3A");
		clear(conn, "MR", "UPDT40001", "UPDT4");

	}

	@Test
	public void test1SimpleUpdateNoNationalIds() throws MpiException {
		
		logger.debug("************* test1SimpleUpdateNoNationalIds ****************");

		String originator = "UPDT1";
		String idBase = originator+"000";
		
		// P1 - Setup a person with no national ids - UKRDC will be generated
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ANT").setGivenName("ADAM").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().startsWith("50")); // Allocated numbers will start with 50 whereas numbers sent in from test stub begin RR 
		assert(natId.getId().length()==9);      // Allocated numbers are 9 characters long 
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		MasterRecord allocatedMr = MasterRecordDAO.get(conn, links.get(0).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));

		logger.debug("************* test1SimpleUpdateNoNationalIds ********UT1-1********");
		// UT1-1 - Simple Update - change surname - still no national ids - should retain the UKRDC from first run
		p1.setSurname("ANT2").setPostcode("CH1 6LB").setStreet("Townfield Lane");
		NationalIdentity natId2 = store(p1);
		// VERIFY
		assert(natId2!=null);
		assert(natId2.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId2.getId().equals(natId.getId()));
		assert(natId2.getId().startsWith("50")); // Allocated numbers will start with 50 whereas numbers sent in from test stub begin RR 
		assert(natId2.getId().length()==9);      // Allocated numbers are 9 characters long 
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		allocatedMr = MasterRecordDAO.get(conn, links.get(0).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));
		assert(person.getSurname().equals("ANT2"));
		assert(person.getPrevSurname().equals("ANT"));

	}

	@Test
	public void test2UpdateAddingUKRDCId() throws MpiException {

		logger.debug("************* test2UpdateAddingUKRDCId ****************");

		String originator = "UPDT2";
		String idBase = originator+"000";
		// P1 - Setup a person with no national ids
		Person p1 = new Person().setDateOfBirth(d1).setSurname("BOLD").setGivenName("BRENDA").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		NationalIdentity natId = store(p1);
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().startsWith("50")); // Allocated numbers will start with 50 whereas numbers sent in from test stub begin RR 
		assert(natId.getId().length()==9);      // Allocated numbers are 9 characters long 
		
		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-1********");
		// UT2-1 - Add a UKRDC id
		p1.setId(0); // reset the id - incoming record won't have this.
		String ukrdcId = idBase+"R1";
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId);
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2********");
		// UT2-2 - No UKRDC on inbound, but already on record - just leave it there - this may be a common occurrence
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2A********");
		// UT2-2 - UKRDC on inbound matching that on on record 
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R1");
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-3********");
		// UT2-3 - As 2 but change demographics - later effective date (not set, so defaulted) should cause master update
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDA");
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getEffectiveDate().compareTo(p1.getEffectiveDate())==0);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-4********");
		// UT2-4 - As 2 but change demographics - earlier effective date - no update of Master. Match still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		//Chcek that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-5********");
		// UT2-5 - As 4 change demographics - earlier effective date - no update of Master. But no longer verifies so work item raised and master marked for investigation
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setDateOfBirth(d4);
		p1.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		//Check that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		assert(master.getStatus()==MasterRecord.INVESTIGATE);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==(WorkItemType.TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY));

	}
	
	@Test
	public void test3UpdateChangingUKRDCId() throws MpiException {

		logger.debug("************* test3UpdateChangingUKRDCId ****************");

		String originator = "UPDT3";
		String originator2 = "UPDT3A";
		String idBase = originator+"000";
		String idBase2 = originator2+"00";
		String ukrdcId1 = idBase+"R1";
		String ukrdcId2 = idBase+"R2";
		String ukrdcId3 = idBase+"R3";

		// P1 - Setup a person with a UKRDC
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId1);
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		
		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-1********");
		// UT3-1 - Change the UKRDC ID. Should delete the link to the previous and the original master whilst creating new links
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId2);
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId2));
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, ukrdcId1,NationalIdentity.UKRDC_TYPE);
		assert(master==null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId(conn, ukrdcId2,NationalIdentity.UKRDC_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-2********");
		// P2 - Setup another person linked to the same UKRDC
		Person p2 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId2);
		natId = store(p2);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId2));

		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-2********");
		// UT3-2 - Change the UKRDC ID. Should delete the link to the previous. Original master stays in place though because p2 is linked to it
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId3);
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId3));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(conn, ukrdcId2,NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		master2 = MasterRecordDAO.findByNationalId(conn, ukrdcId3,NationalIdentity.UKRDC_TYPE);
		assert(master2!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);
	}
	
	@Test
	public void test4AddingOtherNationalId() throws MpiException {

		logger.debug("************* test4AddingOtherNationalId ****************");

		String originator = "UPDT4";
		String idBase = originator+"000";
		String ukrdcId1 = idBase+"R1";

		// P1 - Setup a person with a UKRDC
		logger.debug("************* test4AddingOtherNationalId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("DERBY").setGivenName("DORIS").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId1);
		NationalIdentity natId = store(p1);
		// VERIFY SETUP
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-1********");
		// UT4-1 - Add NHS Number and CHI Number
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1"));
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord masterUkrdc = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(masterUkrdc!=null);
		MasterRecord masterNhs = MasterRecordDAO.findByNationalId(conn, idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		MasterRecord masterChi = MasterRecordDAO.findByNationalId(conn, idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi!=null);

		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterChi.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-2********");
		// UT4-2 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,idBase+"H1"));
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		masterUkrdc = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(masterUkrdc!=null);
		masterNhs = MasterRecordDAO.findByNationalId(conn, idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		masterChi = MasterRecordDAO.findByNationalId(conn, idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		MasterRecord masterHsc = MasterRecordDAO.findByNationalId(conn, idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);

		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		logger.debug("************* test4AddingOtherNationalId ********UT4-3********");
		// UT4-3 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBY");
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(conn, idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(conn, idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(conn, idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);		

		logger.debug("************* test4AddingOtherNationalId ********UT4-4********");
		// UT4-4 - Update the surname - but set an old effective date. Master records should not be updated. Work Items not created as data still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBEY");
		p1.setEffectiveDate(getDate("2017-08-01"));
		natId = store(p1);
		// VERIFY
		assert(natId!=null);
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId.getId().equals(ukrdcId1));
		person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBEY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(conn, idBase+"R1",NationalIdentity.UKRDC_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(conn, idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(conn, idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(conn, idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);		

	}
	
}
