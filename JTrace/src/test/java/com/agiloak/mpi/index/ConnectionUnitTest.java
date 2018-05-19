package com.agiloak.mpi.index;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

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
		try {
			SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE2");
			Connection conn = SimpleConnectionManager.getDBConnection();
			assert(false);
		} catch (MpiException ex) {
			assert(true);
		}
	}

	@Test
	public void testConfiguredConnection() throws MpiException, SQLException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		
		Connection conn1 = SimpleConnectionManager.getDBConnection();
		assert(conn1!=null);
		assert(conn1.isValid(1));
		
		Connection conn2 = SimpleConnectionManager.getDBConnection();
		assert(conn2!=conn1);
		assert(conn2!=null);
		assert(conn2.isValid(1));
		
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
