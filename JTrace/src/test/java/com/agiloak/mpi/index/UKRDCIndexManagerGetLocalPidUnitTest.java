package com.agiloak.mpi.index;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.NationalIdentity;
import com.agiloak.mpi.index.Person;
import com.agiloak.mpi.index.PidXREF;
import com.agiloak.mpi.index.UKRDCIndexManager;
import com.agiloak.mpi.index.UKRDCIndexManagerResponse;
import com.agiloak.mpi.index.persistence.JTraceTest;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.index.persistence.PidXREFDAO;

public class UKRDCIndexManagerGetLocalPidUnitTest extends JTraceTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public final static String TEST_EXTRACT  = "TEST";
	public final static String TEST_FACILITY_1  = "GLP01";  
	public final static String TEST_LOCALID_6  = "1000000006";
	public final static String TEST_LOCALID_7  = "1000000007";
	public final static String TEST_LOCALID_7A = "A000000007";
	public final static String TEST_LOCALID_8  = "1000000008";
	public final static String TEST_LOCALID_8A = "A000000008";
	public final static String TEST_LOCALID_9  = "1000000009";
	public final static String TEST_LOCALID_9A = "A000000009";
	
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		// delete test data
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_6);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_7);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_7A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_8);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_8A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_9);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_9A);
		MasterRecordDAO.deleteByNationalId("NHSX000006", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSX000007", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSX000008", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSX000009", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSX00009A", "NHS");
		
	}
	

	/* 
	 * Test find of a local record which already exists 
	 * DIRECT MATCH ON full Local ID
	 * ==> RETURNS LPID of this record
	 * 
	 */
	@Test
	public void testFindMatchExisting() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_6);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000006"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSX000006").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		
		LinkRecord lr = new LinkRecord(mr.getId(), person.getId());
		lr.setUpdatedBy("Nick");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(lr);

		// TEST 1
		Person person2 = new Person();
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_6).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSX000006"));
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		person2.setUnconsolidatedLocalId(person2.getLocalId());
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}

	/* 
	 * Test find of a local record with demographic mismatch
	 * MATCH ON NHS NUMBER BY SE & SF
	 * DEMOGRAPHICS DONT MATCH
	 * ==> RETURN REJECT ==> FAIL
	 * 
	 */
	@Test
	public void testFindMatchReject() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_7);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000007"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSX000007").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		
		LinkRecord lr = new LinkRecord(mr.getId(), person.getId());
		lr.setUpdatedBy("Nick");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(lr);

		// TEST 1
		Person person2 = new Person();
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_7A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSX000007"));
		person2.setTitle("MR").setGivenName("MATT").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		person2.setUnconsolidatedLocalId(person2.getLocalId());
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		
	}

	/* 
	 * Test successful find of a local record
	 * MATCH ON NHS NUMBER BY SE & SF
	 * DEMOGRAPHICS MATCH
	 * ==> RETURN LPID OF MATCHING NUMBER
	 * 
	 */
	@Test
	public void testFindMatchMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_8);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000008"));
		person.setTitle("MR").setGivenName("NICK").setOtherGivenNames("IOAN").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("NICK").setSurname("JONES");
		mr.setNationalId("NHSX000008").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		
		LinkRecord lr = new LinkRecord(mr.getId(), person.getId());
		lr.setUpdatedBy("NICK");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(lr);

		// TEST 1
		Person person2 = new Person();
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_8A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSX000008"));
		person2.setTitle("MR").setGivenName("NICK").setOtherGivenNames("IOAN").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		person2.setUnconsolidatedLocalId(person2.getLocalId());
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}

	/* 
	 * Test find of a local record with mismatch 
	 * NO MATCH ON NHS NUMBER BY SE & SF
	 * ==> RETURN NEW OF MATCHING NUMBER
	 * 
	 */
	@Test
	public void testFindMatchNoMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_9);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000009"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSX000009").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		
		LinkRecord lr = new LinkRecord(mr.getId(), person.getId());
		lr.setUpdatedBy("Nick");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(lr);

		// TEST 1
		Person person2 = new Person();
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_9A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSX00009A"));
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		person2.setUnconsolidatedLocalId(person2.getLocalId());
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals("NEW"));
		
	}

	
}
