package com.agiloak.mpi.index;

import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerLinkSystemTest extends UKRDCIndexManagerBaseTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Date d1 = getDate("1962-08-31");

	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();

		clear(conn, "MR", "LNKT10001", "LNKT1");
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000R1",NationalIdentity.UKRDC_TYPE);
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000N1",NationalIdentity.NHS_TYPE);
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000C1",NationalIdentity.CHI_TYPE);
		clear(conn, "MR", "LNKT20001", "LNKT2");

		clear(conn, "MR", "LNKT10002", "LNKT1");
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000R2",NationalIdentity.UKRDC_TYPE);
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000N2",NationalIdentity.NHS_TYPE);
		MasterRecordDAO.deleteByNationalId(conn, "LNKT1000C2",NationalIdentity.CHI_TYPE);
		clear(conn, "MR", "LNKT20002", "LNKT2");
	}

	@Test
	public void testManualLink() throws MpiException {

		String originator = "LNKT1";
		String originator2 = "LNKT2";
		String idBase = originator+"000";
		String idBase2 = originator2+"000";

		UKRDCIndexManager im = new UKRDCIndexManager();
		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1");
		
		// Setup person with UKRR number, CHI and NHS numbers
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R1");
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		im.store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord ukrdcMaster = MasterRecordDAO.findByNationalId(conn, idBase+"R1", NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// Setup person with no national links - will allocate the UKRDC Number
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		im.store(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		MasterRecord allocatedMr = MasterRecordDAO.get(conn, links.get(0).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// LT1-1 - Link to UKRDC
		// LT1-3 - Deletes prior link
		UKRDCIndexManagerResponse resp = im.link(p2.getId(), ukrdcMaster.getId(), "NJONES01", 1, "Verified with Saughall Medical Centre.");
		// VERIFY
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		LinkRecord newLink = LinkRecordDAO.find(conn, ukrdcMaster.getId(), p2.getId());
		assert(newLink!=null);
		assert(newLink.getMasterId()==ukrdcMaster.getId());
		assert(newLink.getUpdatedBy().equals("NJONES01"));
		assert(newLink.getLinkType()==LinkRecord.MANUAL_TYPE);
		// LT1-4 - Deletes prior link
		MasterRecord mr = MasterRecordDAO.get(conn, allocatedMr.getId());
		assert(mr==null);
		LinkRecord oldLink = LinkRecordDAO.find(conn, allocatedMr.getId(), p2.getId());
		assert(oldLink==null);

	}
	
	@Test
	public void testManualLinkFailure() throws MpiException {

		String originator = "LNKT1";
		String originator2 = "LNKT2";
		String idBase = originator+"000";
		String idBase2 = originator2+"000";

		UKRDCIndexManager im = new UKRDCIndexManager();
		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N2");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C2");
		
		// Setup person with UKRR number, CHI and NHS numbers
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"2").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R2");
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		im.store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord ukrdcMaster = MasterRecordDAO.findByNationalId(conn, idBase+"R2", NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// Setup person with no national links - will allocate the UKRDC Number
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"2").setLocalIdType("MR").setOriginator(originator2);
		im.store(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==1);
		MasterRecord allocatedMr = MasterRecordDAO.get(conn, links.get(0).getMasterId());
		assert(allocatedMr.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE));
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// LT1-1 - Link to UKRDC
		// LT1-3 - Deletes prior link
		// FORCE ERROR ON LINK - MAKE THE updatedBy > 20
		UKRDCIndexManagerResponse resp = im.link(p2.getId(), ukrdcMaster.getId(), "1234567890123456789012345", 1, "Verified with Saughall Medical Centre.");
		// VERIFY
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		LinkRecord newLink = LinkRecordDAO.find(conn, ukrdcMaster.getId(), p2.getId());
		assert(newLink==null);
		// LT1-4 - Deletes prior link
		MasterRecord mr = MasterRecordDAO.get(conn, allocatedMr.getId());
		assert(mr!=null);
		LinkRecord oldLink = LinkRecordDAO.find(conn, allocatedMr.getId(), p2.getId());
		assert(oldLink!=null);

	}
	
	@Test
	public void testLinkValidationUser1() {
		// LT1-2A - No User
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.link(1,1, null, 1, "Verified with Saughall Medical Centre.");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testLinkValidationUser2() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2B - No User
		UKRDCIndexManagerResponse resp = im.link(1,1, "", 1, "Verified with Saughall Medical Centre.");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testLinkValidationDesc1() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2C - No Desc (null)
		UKRDCIndexManagerResponse resp = im.link(1,1, "NJONES01", 1, null);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
	@Test
	public void testLinkValidationDesc2() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2D - No Desc
		UKRDCIndexManagerResponse resp = im.link(1,1, "NJONES01", 1, null);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testLinkValidationPersonId() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2E - No PersonId
		UKRDCIndexManagerResponse resp = im.link(0,1, "NJONES01", 1, "TEST");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
	@Test
	public void testLinkValidationMasterId() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2F - No MasterId
		UKRDCIndexManagerResponse resp = im.link(1,0, "NJONES01", 1, "TEST");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
	@Test
	public void testLinkValidationLinkType() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		// LT1-2F - No MasterId
		UKRDCIndexManagerResponse resp = im.link(1,1, "NJONES01", 0, "TEST");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
}
