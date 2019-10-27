package com.agiloak.mpi.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.index.persistence.PidXREFDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

// Depends on the Database existing. Should probably be a DB rebuild as part of this test

public class ConnectionUnitTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	@Test
	public void testDefaultConnection() throws MpiException, SQLException {
		SimpleConnectionManager.reset();
		Connection conn1 = SimpleConnectionManager.getDBConnection();
		assert(conn1!=null);
		assert(conn1.isValid(1));
		
		Connection conn2 = SimpleConnectionManager.getDBConnection();
		assert(conn2!=conn1);
		assert(conn2!=null);
		assert(conn2.isValid(1));
	}
	
	@Test
	public void testConfiguredConnectionBadDatabase() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE2");
		SimpleConnectionManager.getDBConnection();
	}

	@Test
	public void testConfiguredConnectionBadSchema() throws MpiException, SQLException {
		exception.expect(PSQLException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		// Connection is made even though the schema does not exist (odd but true it appears)
		assert(conn.isValid(1));
		// But no data is available so a simple query will fail
		checkPersonTable(conn);
	}

	@Test
	public void testConfiguredConnectionBadSchemaAuditDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		AuditDAO.findByPerson(conn, 1);
	}

	@Test
	public void testConfiguredConnectionBadSchemaLinkRecordDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		LinkRecordDAO.findByPerson(conn, 1);
	}

	@Test
	public void testConfiguredConnectionBadSchemaMasterRecordDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		MasterRecordDAO.get(conn, 1);
	}

	@Test
	public void testConfiguredConnectionBadSchemaPersonDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		PersonDAO.findByMasterId(conn, 1);
	}

	@Test
	public void testConfiguredConnectionBadSchemaTraceDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		TraceDAO.getResponse(conn, "");
	}

	@Test
	public void testConfiguredConnectionBadSchemaWorkItemDAO() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		WorkItemDAO.findByPerson(conn, 1);
	}

	@Test
	public void testConfiguredConnectionBadSchemaPidXREF() throws MpiException, SQLException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace2", 10);
		Connection conn = SimpleConnectionManager.getDBConnection();
		PidXREFDAO.findByLocalId(conn, "TEST", "TEST", "12345");
	}

	private void checkPersonTable(Connection conn) throws SQLException {
		PreparedStatement preparedStatement = conn.prepareStatement("Select count(*) from person");
		ResultSet rs = preparedStatement.executeQuery();
		rs.next();
	}

	// Depends on the jtrace schema being in the search path
	@Test
	public void testConfiguredConnection() throws MpiException, SQLException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		
		Connection conn1 = SimpleConnectionManager.getDBConnection();
		assert(conn1!=null);
		assert(conn1.isValid(1));
		checkPersonTable(conn1);
		
		Connection conn2 = SimpleConnectionManager.getDBConnection();
		assert(conn2!=conn1);
		assert(conn2!=null);
		assert(conn2.isValid(1));
		checkPersonTable(conn2);
		
	}
	
	@Test
	public void testConfiguredConnectionWithSchema() throws MpiException, SQLException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE", "jtrace", 10);
		
		Connection conn1 = SimpleConnectionManager.getDBConnection();
		assert(conn1!=null);
		assert(conn1.isValid(1));
		checkPersonTable(conn1);
		
		Connection conn2 = SimpleConnectionManager.getDBConnection();
		assert(conn2!=conn1);
		assert(conn2!=null);
		assert(conn2.isValid(1));
		checkPersonTable(conn1);
		
	}
	
	@Test
	public void testNullParm1() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure(null,"B","C","D","E");
	}
	@Test
	public void testNullParm2() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A",null,"C","D","E");
	}
	@Test
	public void testNullParm3() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B",null,"D","E");
	}
	@Test
	public void testNullParm4() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B","C",null,"E");
	}
	@Test
	public void testNullParm5() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B","C","D",null);
	}

	@Test
	public void testEmptyParm1() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("","B","C","D","E");
	}
	@Test
	public void testEmptyParm2() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","","C","D","E");
	}
	@Test
	public void testEmptyParm3() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B","","D","E");
	}
	@Test
	public void testEmptyParm4() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B","C","","E");
	}
	@Test
	public void testEmptyParm5() throws MpiException {
		exception.expect(MpiException.class);
		SimpleConnectionManager.configure("A","B","C","D","");
	}


}
