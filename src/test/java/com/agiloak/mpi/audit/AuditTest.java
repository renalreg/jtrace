package com.agiloak.mpi.audit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.persistence.AuditDAO;

public class AuditTest {

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
	public static void setup() throws MpiException {
		AuditDAO.deleteByPerson(conn, 100001);
		AuditDAO.deleteByPerson(conn, 100002);
		AuditDAO.deleteByPerson(conn, 100003);
		AuditDAO.deleteByPerson(conn, 100004);
		AuditDAO.deleteByPerson(conn, 100005);
	}

	@Test
	public void testCreateAndRead1() throws MpiException {
		AuditManager am = new AuditManager();
				
		Audit audit = am.create(conn, Audit.NO_MATCH_ASSIGN_NEW, 100001, 100001, "test", "user");
		List<Audit> audits = AuditDAO.findByPerson(conn, 100001);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
		assert(audit2.getUpdatedBy().equals(audit.getUpdatedBy()));

	}
	@Test
	public void testCreateAndRead1WithAttributes() throws MpiException {
		AuditManager am = new AuditManager();
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		
		Audit audit = am.create(conn, Audit.NO_MATCH_ASSIGN_NEW, 100002, 1, "test", "user", attr);
		List<Audit> audits = AuditDAO.findByPerson(conn, 100002);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getUpdatedBy().equals(audit.getUpdatedBy()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
		assert(audit2.getAttributes().size()==2);
		assert(audit2.getAttributes().get("TestK1").equals("TestV1"));
		assert(audit2.getAttributes().get("TestK2").equals("TestV2"));

	}

	@Test
	public void testCreateAndRead2() throws MpiException {
		AuditManager am = new AuditManager();

		Audit audit = am.create(conn, Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 100003, 1, "test", "user");
		List<Audit> audits = AuditDAO.findByPerson(conn, 100003);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getUpdatedBy().equals(audit.getUpdatedBy()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
	}

	@Test
	public void testCreateAndRead3() throws MpiException {
		AuditManager am = new AuditManager();

		Audit audit = am.create(conn, Audit.UKRDC_MERGE, 100004, 1, "test", "user");
		List<Audit> audits = AuditDAO.findByPerson(conn, 100004);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getUpdatedBy().equals(audit.getUpdatedBy()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
	}

	@Test
	public void testCreateNoPerson() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(conn, Audit.NO_MATCH_ASSIGN_NEW,0,1,"test","user");
	}

	@Test
	public void testCreateNoMaster() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(conn, Audit.NO_MATCH_ASSIGN_NEW,1,0,"test","user");
	}
	@Test
	public void testCreateNullDesc() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(conn, Audit.NO_MATCH_ASSIGN_NEW,1,2,null,"user");
	}

	@Test
	public void testCreateEmptyDesc() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(conn, Audit.NO_MATCH_ASSIGN_NEW,1,3,"","user");
	}
	
	@Test
	public void testDelete() throws MpiException {
		AuditManager am = new AuditManager();
		am.create(conn, Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 100005, 1, "test", "user");
		am.create(conn, Audit.NO_MATCH_ASSIGN_NEW, 100005, 2, "test2","user");
		List<Audit> audits = AuditDAO.findByPerson(conn, 100005);
		assert(audits.size()==2);
		
		AuditDAO.deleteByPerson(conn, 100005);
		audits = AuditDAO.findByPerson(conn, 100005);
		assert(audits.size()==0);
		
	}
	
}
