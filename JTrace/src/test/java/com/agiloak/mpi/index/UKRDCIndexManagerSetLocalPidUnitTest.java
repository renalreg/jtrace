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

public class UKRDCIndexManagerSetLocalPidUnitTest extends JTraceTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public final static String TEST_EXTRACT  = "TEST";
	public final static String TEST_FACILITY_1  = "SLP01";  
	public final static String TEST_LOCALID_1  = "1000000001";
	public final static String TEST_LOCALID_2  = "1000000002";
	public final static String TEST_LOCALID_2A = "A000000002";
	public final static String TEST_LOCALID_3  = "1000000003";
	public final static String TEST_LOCALID_3A = "A000000003";
	public final static String TEST_LOCALID_4  = "1000000004";
	public final static String TEST_LOCALID_4A = "A000000004";
	public final static String TEST_LOCALID_5  = "1000000005";
	public final static String TEST_LOCALID_5A = "A000000005";
	public final static String TEST_LOCALID_6  = "1000000006";
	public final static String TEST_LOCALID_6A = "A000000006";
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
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_1);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_2);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_2A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_3);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_3A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_4);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_4A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_5);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_5A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_6);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_6A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_7);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_7A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_8);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_8A);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_9);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_9A);
		MasterRecordDAO.deleteByNationalId("NHSSLP0001", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0002", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0003", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP003A", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0004", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0005", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0006", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0007", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0008", "NHS");
		MasterRecordDAO.deleteByNationalId("NHSSLP0009", "NHS");
		
	}

	@Test
	public void testUpdateMatchExisting() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_1);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0001"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0001").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_1).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0001"));
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}
	
	@Test
	public void testUpdateNoNationalIds() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_2);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0002"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0002").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_2A).setLocalIdType("MR");
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(!resp.getPid().equals(pidx.getPid()));
		assert(resp.getPid().length()==10);
		
		int p1 = Integer.parseInt(pidx.getPid());
		int p2 = Integer.parseInt(resp.getPid());
		assert((p2-p1)==1);
		PidXREF newXref = PidXREFDAO.findByLocalId(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_2A);
		assert(newXref.getPid().equals(resp.getPid()));
		
	}
	
	@Test
	public void testUpdateNoNationalIdMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_3);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0003"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0003").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_3A).setLocalIdType("MR");
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP003A"));
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(!resp.getPid().equals(pidx.getPid()));
		assert(resp.getPid().length()==10);
		
		int p1 = Integer.parseInt(pidx.getPid());
		int p2 = Integer.parseInt(resp.getPid());
		assert((p2-p1)==1);
		PidXREF newXref = PidXREFDAO.findByLocalId(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_3A);
		assert(newXref.getPid().equals(resp.getPid()));
		
	}
	
	@Test
	public void testUpdateMatchRejectGN() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_4);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0004"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0004").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_4A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0004"));
		person2.setTitle("MR").setGivenName("MATT").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getPid()==null);
		
	}
	@Test
	public void testUpdateMatchRejectSN() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_5);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0005"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0005").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_5A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0005"));
		person2.setTitle("MR").setGivenName("NICK").setSurname("SMITH");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getPid()==null);
		
	}
	@Test
	public void testUpdateMatchRejectGender() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_6);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0006"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0006").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_6A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0006"));
		person2.setTitle("MRS").setGivenName("NICK").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("2");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getPid()==null);
		
	}
	@Test
	public void testUpdateMatchRejectDOB() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_7);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0007"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0007").setNationalIdType("NHS");
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
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0007"));
		person2.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person2.setDateOfBirth(getDate("1963-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getPid()==null);
		
	}
	@Test
	public void testUpdateMatchRejectAll() throws MpiException {
		//Mismatch on all demographics to test attribute creation
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_8);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0008"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("NICK").setSurname("Jones");
		mr.setNationalId("NHSSLP0008").setNationalIdType("NHS");
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
		person2.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_8A).setLocalIdType("MR");
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0008"));
		person2.setTitle("MRS").setGivenName("KAREN").setSurname("SMITH");
		person2.setDateOfBirth(getDate("1963-08-31"));
		person2.setGender("2");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getPid()==null);
		
	}

	@Test
	public void testFindMatchMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_9);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSSLP0009"));
		person.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSSLP0009").setNationalIdType("NHS");
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
		person2.addNationalId(new NationalIdentity("NHS", "NHSSLP0009"));
		person2.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.setLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}



	
}
