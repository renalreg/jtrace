package com.agiloak.mpi.index.persistence;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.PidXREF;

public class PidXREFTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public final static String TEST_EXTRACT  = "TEST";
	public final static String TEST_FACILITY_1  = "RZZ01";
	public final static String TEST_LOCALID_1  = "1000000001";
	public final static String TEST_LOCALID_2  = "1000000002";
	public final static String TEST_LOCALID_3  = "1000000003";
	public final static String TEST_LOCALID_4  = "1000000004";
	
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		// delete test data
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_1);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_2);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_3);
		PidXREFDAO.deleteByLocalId(TEST_FACILITY_1,TEST_EXTRACT, TEST_LOCALID_4);
		
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
	
	
}
