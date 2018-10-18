package com.agiloak.mpi.index.persistence;

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

public class PidXREFTest extends JTraceTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public final static String TEST_EXTRACT  = "TEST";
	public final static String TEST_FACILITY_1  = "RZZ01";
	public final static String TEST_LOCALID_1  = "1000000001";
	public final static String TEST_LOCALID_2  = "1000000002";
	public final static String TEST_LOCALID_3  = "1000000003";
	public final static String TEST_LOCALID_4  = "1000000004";
	public final static String TEST_LOCALID_5  = "1000000005";
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
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_1);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_2);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_3);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_4);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_5);
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
	
	@Test
	public void testGetSequence() throws MpiException {
		String patientId = PidXREFDAO.allocate();
		assert(patientId!=null);
		assert(patientId.length()==9);
	}
	
	@Test
	public void testCreate() throws MpiException {
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_1);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);
	}
	
	@Test
	public void testGet() throws MpiException {
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_2);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);
		
		PidXREF pidx2 = PidXREFDAO.get(pidx.getId());
		assert(pidx2.getId()==(pidx.getId()));
		assert(pidx2.getPid().equals(pidx.getPid()));
		assert(pidx2.getSendingFacility().equals(pidx.getSendingFacility()));
		assert(pidx2.getSendingExtract().equals(pidx.getSendingExtract()));
		assert(pidx2.getLocalId().equals(pidx.getLocalId()));
		
	}
	@Test
	public void testDeleteByLocalId() throws MpiException {
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_3);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);
		
		PidXREF pidx2 = PidXREFDAO.get(pidx.getId());
		assert(pidx2.getId()==(pidx.getId()));

		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_3);

		PidXREF pidx3 = PidXREFDAO.get(pidx.getId());
		assert(pidx3==null);
		
	}
	@Test
	public void testCreateDuplicate() throws MpiException {
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_4);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		// Creating a record with the same details should still create a new PidXREF with an allocated PID
		PidXREF pidx2 = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_4);
		PidXREFDAO.create(pidx2);
		assert(pidx2.getId()!=(pidx.getId()));
		assert(!pidx2.getPid().equals(pidx.getPid()));
		assert(pidx2.getSendingFacility().equals(pidx.getSendingFacility()));
		assert(pidx2.getSendingExtract().equals(pidx.getSendingExtract()));
		assert(pidx2.getLocalId().equals(pidx.getLocalId()));

	}
	@Test
	public void testFindByLocalId() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_5);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);
		
		PidXREF pidx2 = PidXREFDAO.findByLocalId(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_5);
		assert(pidx2.getId()==(pidx.getId()));
		assert(pidx2.getPid().equals(pidx.getPid()));
		assert(pidx2.getSendingFacility().equals(pidx.getSendingFacility()));
		assert(pidx2.getSendingExtract().equals(pidx.getSendingExtract()));
		assert(pidx2.getLocalId().equals(pidx.getLocalId()));

	}

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
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}

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
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals("REJECT"));
		
	}

	@Test
	public void testFindMatchMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_8);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000008"));
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		PersonDAO.create(person);

		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHSX000008").setNationalIdType("NHS");
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
		person2.addNationalId(new NationalIdentity("NHS", "NHSX000008"));
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setGender("1");
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals(pidx.getPid()));
		
	}

	@Test
	public void testFindMatchNoMatch() throws MpiException {
		
		PidXREF pidx = new PidXREF(TEST_FACILITY_1, TEST_EXTRACT, TEST_LOCALID_9);
		PidXREFDAO.create(pidx);
		assert(pidx.getId()>0);

		Person person = new Person();
		person.setOriginator(TEST_FACILITY_1).setLocalId(pidx.getPid()).setLocalIdType("MR");
		person.addNationalId(new NationalIdentity("NHS", "NHSX000008"));
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
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getLocalPID(person2, TEST_FACILITY_1, TEST_EXTRACT);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp.getPid().equals("NEW"));
		
	}

	
}
