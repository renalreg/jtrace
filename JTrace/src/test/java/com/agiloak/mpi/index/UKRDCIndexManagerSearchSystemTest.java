package com.agiloak.mpi.index;

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

public class UKRDCIndexManagerSearchSystemTest extends UKRDCIndexManagerBaseTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	//private Date d1 = getDate("1962-08-31");
	private String d1 = "1962-08-31";
	private String d2 = "1962-08-30";
	private String d3 = "1961-08-30";

	@BeforeClass
	public static void setup()  throws MpiException {

		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		clear( "SRCT10001", "SRCT1");
		MasterRecordDAO.deleteByNationalId("SRCT1000R1",NationalIdentity.UKRDC_TYPE);
		clear( "SRCT40001", "SRCT4");
		MasterRecordDAO.deleteByNationalId("SRCT4000R1",NationalIdentity.UKRDC_TYPE);
		clear( "SRCT50001", "SRCT5");
		MasterRecordDAO.deleteByNationalId("SRCT5000R1",NationalIdentity.UKRDC_TYPE);
		clear( "SRCT60001", "SRCT6");
		MasterRecordDAO.deleteByNationalId("SRCT6000R1",NationalIdentity.UKRDC_TYPE);

	}

	@Test
	public void testSearchSimpleMatch() throws MpiException {

		String originator = "SRCT1";
		String idBase = originator+"000";

		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1");
		
		// P1 NationalId for P1. New Person, New Masters for UKRDC, NHS and CHI
		Person p1 = new Person().setDateOfBirth(getDate(d1)).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R1");
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1", NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		//assert(links.get(0).getMasterId()==master.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// ST1-1 - Happy day - perfect match by NHS Number
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS");
		psr.setNationalId(nhs1);
		String ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId!=null);
		assert(ukrdcId.equals(idBase+"R1"));

		// ST1-2 - Found but not verified - dob only 1 part match
		psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d3).setSurname("JONES").setGivenName("NICHOLAS");
		psr.setNationalId(nhs1);
		ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId==null);

		// ST1-3 - Found and verified - dob 2 part match and 3 char SN + 1 char GN match
		psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d2).setSurname("JONAS").setGivenName("NORTON");
		psr.setNationalId(nhs1);
		ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId.equals(idBase+"R1"));

		// ST1-4 - Found and not verified - dob 2 part match and 3 char SN + 0 char GN match
		psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d2).setSurname("JONAS").setGivenName("HORTON");
		psr.setNationalId(nhs1);
		ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId==null);

		// ST1-5 - Found and not verified - dob 2 part match and 2 char SN + 1 char GN match
		psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d2).setSurname("JOHNSON").setGivenName("NORTON");
		psr.setNationalId(nhs1);
		ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId==null);

	}

	@Test
	public void testSearchValidation1() {

		// ST2-1 - No National Id
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS");
		UKRDCIndexManagerResponse resp = search(psr);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		
	}

	@Test
	public void testSearchValidation2() {

		// ST3-1 - No Search Request
		ProgrammeSearchRequest psr = null;
		UKRDCIndexManagerResponse resp = search(psr);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		
	}

	@Test
	public void testSearchNoNationalIdMatch() throws MpiException {

		String originator = "SRCT4";
		String idBase = originator+"000";

		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");
		NationalIdentity nhs2 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N2");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1");
		
		// P1 NationalId for P1. New Person, New Masters for UKRDC, NHS and CHI
		Person p1 = new Person().setDateOfBirth(getDate(d1)).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R1");
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1", NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// ST4-1 - No match for the NHS Number
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS");
		psr.setNationalId(nhs2);
		String ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId==null);

	}
	
	//@Test -- NOT POSSIBLE FROM V1.1.0 as number is allocated if not found
	public void testSearchNoUKRDCFound() throws MpiException {

		String originator = "SRCT5";
		String idBase = originator+"000";

		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1");
		
		// P1 NationalId for P1. New Person, New Masters for NHS and CHI. Will Allocate a UKRDC number
		Person p1 = new Person().setDateOfBirth(getDate(d1)).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// ST5-1 - Match on NHS Number, but no UKRDC Number is linked - NOT POSSIBLE FROM V1.1.0 as number is allocated if not found
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS");
		psr.setNationalId(nhs1);
		String ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId==null);

	}

	@Test
	public void testSearchRequestBuild() throws MpiException {

		String originator = "SRCT6";
		String idBase = originator+"000";

		NationalIdentity nhs1 = new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1");
		NationalIdentity chi1 = new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1");
		
		// P1 NationalId for P1. New Person, New Masters for UKRDC, NHS and CHI
		Person p1 = new Person().setDateOfBirth(getDate(d1)).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(idBase+"R1");
		p1.addNationalId(nhs1);
		p1.addNationalId(chi1);
		store(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1", NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// ST6-1 - convenience methods (String DOB method and component id/ type)
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setSurname("JONES").setGivenName("NICHOLAS");
		psr.setDateOfBirth("1962-08-31");
		psr.setNationalId(NationalIdentity.NHS_TYPE, idBase+"N1");
		String ukrdcId = search(psr).getNationalIdentity().getId();
		// VERIFY 
		assert(ukrdcId.equals(idBase+"R1"));

	}
	
	@Test
	public void testSearchRequestBuildBadDate() {

		// ST7-1 - bad date
		String originator = "SRCT7";
		String idBase = originator+"000";
		ProgrammeSearchRequest psr = new ProgrammeSearchRequest();
		psr.setNationalId(NationalIdentity.NHS_TYPE, idBase+"N1");
		psr.setDateOfBirth("1962-z-31");
		UKRDCIndexManagerResponse resp = search(psr);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("Date"));

	}
	
	protected UKRDCIndexManagerResponse search(ProgrammeSearchRequest psr) {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.search(psr);
		return resp;
	}

}
