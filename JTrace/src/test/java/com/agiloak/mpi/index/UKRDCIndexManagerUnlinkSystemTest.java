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
import com.agiloak.mpi.workitem.WorkItemManager;
import com.agiloak.mpi.workitem.WorkItemManagerResponse;
import com.agiloak.mpi.workitem.WorkItemStatus;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerUnlinkSystemTest extends UKRDCIndexManagerBaseTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1963-08-30");

	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();

		clearAll(conn);
	}

	// CASE 1 - 2nd record matches on NHS Number but demographics don't match (different DOB and SURNAME). Records linked with work record and master marked
	//          NHS Master now has incorrect SURNAME because of the mismatch
	//@Test
	public void testUnlinkCase1() throws MpiException {

		String originator = "LNKT1";
		String originator2 = "LNKT2";
		String idBase = originator+"001";
		String idBase2 = originator2+"001";
		NationalIdentity nhs = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// Setup person with NHS Number - will allocate UKRDC
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.addNationalId(nhs);
		UKRDCIndexManagerResponse resp1 = im.store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord ukrdcMaster1 = MasterRecordDAO.findByNationalId(conn, resp1.getNationalIdentity().getId(), NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster1!=null);
		MasterRecord nhsMaster1 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster1!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==2);
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// Setup person 2 with same NHS Number - will link to record above with Work Items for 
		// 1 Claimed to NHS Number not verified
		// 2 Inferred claim to UKRDC Number not verified
		// Sets up a new UKRDC Number
		Person p2 = new Person().setDateOfBirth(d2).setSurname("JOHNSON").setGivenName("NICHOLAS").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.addNationalId(nhs);
		UKRDCIndexManagerResponse resp2 = im.store(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(conn, p2.getId());
		assert(links.size()==2);
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==2);
		MasterRecord ukrdcMaster2 = MasterRecordDAO.findByNationalId(conn, resp2.getNationalIdentity().getId(), NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster2!=null);
		MasterRecord nhsMaster2 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster2!=null);
		assert(nhsMaster2.getSurname().equals(p2.getSurname()));
		assert(nhsMaster2.getStatus()==MasterRecord.INVESTIGATE);
		assert(ukrdcMaster1.getId()!=ukrdcMaster2.getId());
		assert(nhsMaster1.getId()==nhsMaster2.getId());

		// UNLINK
		UKRDCIndexManagerResponse unlinkResp = im.unlink(p2.getId(), nhsMaster1.getId(), "NJONES01", "Verified with Saughall Medical Centre.");
		// VERIFY
		assert(unlinkResp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		LinkRecord oldLink = LinkRecordDAO.find(conn, nhsMaster1.getId(), p2.getId()); // 2nd record has matched to previous NHS Number
		assert(oldLink==null);
		nhsMaster2 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster2!=null);
		assert(nhsMaster2.getStatus()==MasterRecord.OK);
		assert(nhsMaster2.getSurname().equals(p1.getSurname()));
		assert(nhsMaster2.getGivenName().equals(p1.getGivenName()));
		assert(nhsMaster2.getGender().equals(p1.getGender()));
		assert(nhsMaster2.getDateOfBirth().compareTo(p1.getDateOfBirth())==0);

	}

	// CASE 2 - 2 totally matching records
	//          Change 1 - Other one doesn't match = TYPE 7 Work Item
	//          
	@Test
	public void testUnlinkCase2() throws MpiException {

		String originator = "LNKT1";
		String originator2 = "LNKT2";
		String idBase = originator+"002";
		String idBase2 = originator2+"002";
		NationalIdentity nhs = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N2");

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// Setup person with NHS Number - will allocate UKRDC
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.addNationalId(nhs);
		UKRDCIndexManagerResponse resp1 = im.store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord ukrdcMaster1 = MasterRecordDAO.findByNationalId(conn, resp1.getNationalIdentity().getId(), NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster1!=null);
		MasterRecord nhsMaster1 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster1!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==2);
		List<WorkItem> items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);

		// Setup person 2 with same NHS Number - will link to record above with Work Items for 
		// 1 Claimed to NHS Number not verified
		// 2 Inferred claim to UKRDC Number not verified
		// Sets up a new UKRDC Number
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.addNationalId(nhs);
		UKRDCIndexManagerResponse resp2 = im.store(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(conn, p2.getId());
		assert(links.size()==2);
		items = WorkItemDAO.findByPerson(conn, person.getId());
		assert(items.size()==0);
		MasterRecord ukrdcMaster2 = MasterRecordDAO.findByNationalId(conn, resp2.getNationalIdentity().getId(), NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster2!=null);
		assert(ukrdcMaster2.getId()==ukrdcMaster1.getId());
		assert(ukrdcMaster2.getStatus()==MasterRecord.OK);
		MasterRecord nhsMaster2 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster2!=null);
		assert(nhsMaster1.getId()==nhsMaster2.getId());
		assert(nhsMaster2.getStatus()==MasterRecord.OK);
		
		// Change the demogs to break the verification. DOB is important, GivenName is just an interesting marker for verification
		p2 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("NICK").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.addNationalId(nhs);
		resp2 = im.store(p2);
		items = WorkItemDAO.findByPerson(conn, p1.getId());
		assert(items.size()==2);

		// UNLINK NHS
		UKRDCIndexManagerResponse unlinkResp = im.unlink(p2.getId(), nhsMaster1.getId(), "NJONES01", "Verified with Saughall Medical Centre.");
		// VERIFY
		assert(unlinkResp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		LinkRecord oldLink = LinkRecordDAO.find(conn, nhsMaster1.getId(), p2.getId()); // 2nd record has matched to previous NHS Number
		assert(oldLink==null);
		nhsMaster2 = MasterRecordDAO.findByNationalId(conn, nhs.getId(), NationalIdentity.NHS_TYPE);
		assert(nhsMaster2!=null);
		assert(nhsMaster2.getStatus()==MasterRecord.OK);
		assert(nhsMaster2.getSurname().equals(p1.getSurname()));
		assert(nhsMaster2.getGivenName().equals(p1.getGivenName()));
		assert(nhsMaster2.getGender().equals(p1.getGender()));
		assert(nhsMaster2.getDateOfBirth().compareTo(p1.getDateOfBirth())==0);

		// UNLINK UKRDC
		UKRDCIndexManagerResponse unlinkResp2 = im.unlink(p2.getId(), ukrdcMaster1.getId(), "NJONES01", "Verified with Saughall Medical Centre.");
		// VERIFY
		assert(unlinkResp2.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		LinkRecord oldLink2 = LinkRecordDAO.find(conn, ukrdcMaster1.getId(), p2.getId()); // 2nd record has matched to previous UKRDC Number
		assert(oldLink2==null);
		// get the original UKRDC id and see if it matches the original person
		MasterRecord ukrdcMaster1a = MasterRecordDAO.findByNationalId(conn, ukrdcMaster1.getNationalId(), NationalIdentity.UKRDC_TYPE);
		assert(ukrdcMaster1a!=null);
		assert(ukrdcMaster1a.getStatus()==MasterRecord.OK);
		assert(ukrdcMaster1a.getSurname().equals(p1.getSurname()));
		assert(ukrdcMaster1a.getGivenName().equals(p1.getGivenName()));
		assert(ukrdcMaster1a.getGender().equals(p1.getGender()));
		assert(ukrdcMaster1a.getDateOfBirth().compareTo(p1.getDateOfBirth())==0);
		
		// CLEAR WorkItems
		WorkItemManager wim = new WorkItemManager();
		WorkItemManagerResponse wiresp1 = wim.update(items.get(0).getId(), WorkItemStatus.STATUS_CLOSED, "Wrong NHS Number causing confusion case", "Nick");
		assert(wiresp1.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		WorkItemManagerResponse wiresp2 = wim.update(items.get(1).getId(), WorkItemStatus.STATUS_CLOSED, "Wrong NHS Number causing confusion case", "Nick");
		assert(wiresp2.getStatus()==UKRDCIndexManagerResponse.SUCCESS);

	}

	@Test
	public void testUnlinkValidationUser1() {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(1,1, null, "Verified with Saughall Medical Centre.");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testUnlinkValidationUser2() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(1,1, "", "Verified with Saughall Medical Centre.");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testUnlinkValidationDesc1() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(1,1, "NJONES01", null);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
	@Test
	public void testUnlinkValidationDesc2() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(1,1, "NJONES01", "");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}

	@Test
	public void testUnlinkValidationPersonId() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(0, 1, "NJONES01", "DESC");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
	@Test
	public void testUnlinkValidationMasterId() throws MpiException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.unlink(1, 0, "NJONES01", "DESC");
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Incomplete parameters"));
	}
	
}
