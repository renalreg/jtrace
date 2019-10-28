package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static org.hamcrest.CoreMatchers.containsString;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.PidXREF;
import com.agiloak.mpi.index.TestIds;

public class PidXREFTest extends JTraceTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public static Connection conn = null;
	@Before
	public void openConnection()  throws MpiException {
		conn = SimpleConnectionManager.getDBConnection();
	}
	@After
	public void closeConnection()  throws MpiException {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new MpiException("FAILED TO CLOSE CONNECTION");
		}
	}

	@BeforeClass
	public static void setupWrapper()  throws MpiException, SQLException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();
		setup();
		conn.close();
	}

	public static void setup()  throws MpiException {
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_1);
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_2);
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_3);
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_4);
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_5);
		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_6);
	}

	@Test
	public void testGetSequence() throws MpiException {
		String patientId = PidXREFDAO.allocate(conn);
		assert(patientId!=null);
		assert(patientId.length()==10);
	}
	@Test
	public void testCheckSequential() throws MpiException {
		String patientId1 = PidXREFDAO.allocate(conn);
		assert(patientId1!=null);
		assert(patientId1.length()==10);
		
		String patientId2 = PidXREFDAO.allocate(conn);
		assert(patientId2!=null);
		assert(patientId2.length()==10);
		
		int p1 = Integer.parseInt(patientId1);
		int p2 = Integer.parseInt(patientId2);
		assert((p2-p1)==1);
	}
	
	@Test
	public void testCreate() throws MpiException {
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_1);
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		assert(pidx.getPid().length()==10);
		assert(pidx.getCreationDate()!=null);
		assert(pidx.getLastUpdated()!=null);
	}
	
	@Test
	public void testCreateWithPid() throws MpiException {
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_6);
		pidx.setPid("TEST1");
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		assert(pidx.getPid().equals("TEST1"));
		assert(pidx.getCreationDate()!=null);
		assert(pidx.getLastUpdated()!=null);
	}

	@Test
	public void testGet() throws MpiException {
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_2);
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		assert(pidx.getCreationDate()!=null);
		assert(pidx.getLastUpdated()!=null);
		
		PidXREF pidx2 = PidXREFDAO.get(conn, pidx.getId());
		assert(pidx2.getId()==(pidx.getId()));
		assert(pidx2.getPid().equals(pidx.getPid()));
		assert(pidx2.getSendingFacility().equals(pidx.getSendingFacility()));
		assert(pidx2.getSendingExtract().equals(pidx.getSendingExtract()));
		assert(pidx2.getLocalId().equals(pidx.getLocalId()));
		assert(pidx2.getCreationDate().compareTo(pidx.getCreationDate())==0);
		assert(pidx2.getLastUpdated().compareTo(pidx.getLastUpdated())==0);
		
	}

	@Test
	public void testDeleteByLocalId() throws MpiException {
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_3);
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		
		PidXREF pidx2 = PidXREFDAO.get(conn, pidx.getId());
		assert(pidx2.getId()==(pidx.getId()));

		PidXREFDAO.deleteByLocalId(conn, TestIds.FACILITY_PIDX,TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_3);

		PidXREF pidx3 = PidXREFDAO.get(conn, pidx.getId());
		assert(pidx3==null);
		
	}

	@Test
	public void testCreateDuplicate() throws MpiException {
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_4);
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		assert(pidx.getCreationDate()!=null);
		assert(pidx.getLastUpdated()!=null);

		// Creating a record with the same details should error
		PidXREF pidx2 = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_4);
		exception.expect(MpiException.class);
		exception.expectMessage(containsString("PidXREF insert failed"));
		PidXREFDAO.create(conn, pidx2);
	}

	@Test
	public void testFindByLocalId() throws MpiException {
		
		PidXREF pidx = new PidXREF(TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_5);
		PidXREFDAO.create(conn, pidx);
		assert(pidx.getId()>0);
		assert(pidx.getCreationDate()!=null);
		assert(pidx.getLastUpdated()!=null);
		
		PidXREF pidx2 = PidXREFDAO.findByLocalId(conn, TestIds.FACILITY_PIDX, TestIds.EXTRACT_TEST1, TestIds.TEST_LOCALID_5);
		assert(pidx2.getId()==(pidx.getId()));
		assert(pidx2.getPid().equals(pidx.getPid()));
		assert(pidx2.getSendingFacility().equals(pidx.getSendingFacility()));
		assert(pidx2.getSendingExtract().equals(pidx.getSendingExtract()));
		assert(pidx2.getLocalId().equals(pidx.getLocalId()));
		assert(pidx2.getCreationDate().compareTo(pidx.getCreationDate())==0);
		assert(pidx2.getLastUpdated().compareTo(pidx.getLastUpdated())==0);

	}
	
}
