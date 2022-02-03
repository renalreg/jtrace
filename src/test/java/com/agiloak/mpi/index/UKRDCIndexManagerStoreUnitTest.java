package com.agiloak.mpi.index;

import java.sql.Connection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.AuditUtility;
import com.agiloak.mpi.index.persistence.JTraceTest;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.index.persistence.PidXREFDAO;

/*
 * NOTE: This is basic testing added for rollback. Testing of store happens in other test classes though maybe not rigorously
 */
public class UKRDCIndexManagerStoreUnitTest extends UKRDCIndexManagerBaseTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	public static Connection conn = null;

	public final static String TEST_EXTRACT  = "TEST";
	public final static String TEST_FACILITY_1  = "STOR1";  
	public final static String TEST_LOCALID_1  = "1000000001";
	public final static String TEST_LOCALID_2  = "1000000002";
	public final static String TEST_NHS_1  = "NHSSTOR001";
	public final static String TEST_NHS_2  = "NHSSTOR002";
	
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		// Get a connection. This will be used for all test setup. 
		// UKRDC Index Manager will get and manage it's own connection
		conn = SimpleConnectionManager.getDBConnection();
		// RESET audit.updatedby to the correct value before starting (just in case)
		AuditUtility.makeUpdatedByMandatory(conn, false);

		// delete test data
		clear(conn, "MR", TEST_LOCALID_1, TEST_FACILITY_1);
		clear(conn, "MR", TEST_LOCALID_2, TEST_FACILITY_1);

	}

	@AfterClass
	public static void cleanup()  throws MpiException {
		// RESET audit.updatedby to the correct value after (just in case)
		AuditUtility.makeUpdatedByMandatory(conn, false);
	}

	@Test
	public void testStore() throws MpiException {
		
		Person p1 = new Person();
		p1.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_1).setLocalIdType("MR");
		p1.addNationalId(new NationalIdentity("NHS", TEST_NHS_1));
		p1.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		p1.setDateOfBirth(getDate("1962-08-31"));
		p1.setGender("1");
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.store(p1);
		
		assert(resp.status == UKRDCIndexManagerResponse.SUCCESS);
		NationalIdentity natId = resp.getNationalIdentity();
		assert(natId.getType()==NationalIdentity.UKRDC_TYPE);
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, natId.getId(), NationalIdentity.UKRDC_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
		assert(links.size()==2);
	}

	@Test
	public void testStoreRollback() throws MpiException {

		Person p1 = new Person();
		p1.setOriginator(TEST_FACILITY_1).setLocalId(TEST_LOCALID_2).setLocalIdType("MR");
		p1.addNationalId(new NationalIdentity("NHS", TEST_NHS_2));
		p1.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		p1.setDateOfBirth(getDate("1962-08-31"));
		p1.setGender("1");
		UKRDCIndexManager im = new UKRDCIndexManager();

		// MAKE audit.updatedby mandatory to cause an error and a rollback
		AuditUtility.makeUpdatedByMandatory(conn, true);
		UKRDCIndexManagerResponse resp = im.store(p1);
		// RESET audit.updatedby to the correct value before starting (just in case)
		AuditUtility.makeUpdatedByMandatory(conn, false);

		assert(resp.status == UKRDCIndexManagerResponse.FAIL);
		assert(resp.nationalIdentity==null);
		Person person = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person==null);
	}
	
}
