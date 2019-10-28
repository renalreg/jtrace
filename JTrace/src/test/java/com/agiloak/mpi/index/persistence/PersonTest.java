package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;
import com.agiloak.mpi.index.TestIds;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class PersonTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	public static Connection conn = null;

	@Before
	public void openConnection() throws MpiException {
		conn = SimpleConnectionManager.getDBConnection();
	}

	@After
	public void closeConnection() throws MpiException {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new MpiException("FAILED TO CLOSE CONNECTION");
		}
	}

	protected static void clear(Connection conn, String localId, String originator) throws MpiException {
		
		Person person = PersonDAO.findByLocalId(conn, "MR", localId, originator);
		if (person != null) {
			LinkRecordDAO.deleteByPerson(conn, person.getId());
			WorkItemDAO.deleteByPerson(conn, person.getId());
			PersonDAO.delete(conn, person);
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
		clear(conn, TestIds.TEST_LOCALID_1, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_2, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_3, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_4, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_5, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_6, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_7, TestIds.ORIG_TEST1);
		clear(conn, TestIds.TEST_LOCALID_8, TestIds.ORIG_TEST1);
		MasterRecordDAO.deleteByNationalId(conn, TestIds.TEST_NHS_1, TestIds.TYPE_NHS);
	}

	@Test
	public void testFindByMasterId() throws MpiException {
		
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_1).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setPrimaryIdType(TestIds.TYPE_NHS).setPrimaryId(TestIds.TEST_NHS_1);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person);
		assert(true);
		Person person2 = new Person();
		person2.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_2).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setPrimaryIdType(TestIds.TYPE_NHS).setPrimaryId(TestIds.TEST_NHS_1);
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setDateOfDeath(getDate("2062-08-31"));
		person2.setGender("1");
		person2.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person2);
		assert(true);

		MasterRecord master = new MasterRecord(person);
		master.setEffectiveDate(new Date());
		MasterRecordDAO.create(conn, master);
		LinkRecord l1 = new LinkRecord(master.getId(), person.getId());
		LinkRecord l2 = new LinkRecord(master.getId(), person2.getId());
		LinkRecordDAO.create(conn, l1);
		LinkRecordDAO.create(conn, l2);
		
		List<Person> personList = PersonDAO.findByMasterId(conn, master.getId());
		assert(personList != null);
		assert(personList.size()==2);
		
	}

	@Test
	public void testRead() throws MpiException {
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_3).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person);
		assert(true);
		
		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_3, TestIds.ORIG_TEST1);
		assert(person2 != null);
		
		assert(safeCompare(person2.getOriginator(),person.getOriginator()));
		assert(safeCompare(person2.getLocalId(),person.getLocalId()));
		assert(safeCompare(person2.getLocalIdType(),person.getLocalIdType()));
		assert(safeCompare(person2.getPrimaryId(),person.getPrimaryId()));
		assert(safeCompare(person2.getPrimaryIdType(),person.getPrimaryIdType()));
		
		assert(safeCompare(person2.getGivenName(),person.getGivenName()));
		assert(safeCompare(person2.getOtherGivenNames(),person.getOtherGivenNames()));
		assert(safeCompare(person2.getSurname(),person.getSurname()));
		assert(safeCompare(person2.getTitle(),person.getTitle()));
		assert(safeCompare(person2.getPrevSurname(),person.getPrevSurname()));

		assert(safeCompare(person2.getPostcode(),person.getPostcode()));
		assert(safeCompare(person2.getStreet(),person.getStreet()));

		assert(safeCompare(person2.getGender(),person.getGender()));
		assert(safeCompare(person2.getDateOfBirth(),person.getDateOfBirth()));
		assert(safeCompare(person2.getDateOfDeath(),person.getDateOfDeath()));
		
	}

	@Test
	public void testReadNTF() throws MpiException {
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_4).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person);
		
		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_4, TestIds.ORIG_ANO);
		assert(person2 == null);
	}

	@Test
	public void testDelete() throws MpiException {
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_5).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person);
		
		PersonDAO.delete(conn, person);
		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_5, TestIds.ORIG_TEST1);
		assert(person2 ==null);
	}

	@Test
	public void testStore() throws MpiException {
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_6).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(conn, person);

		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_6, TestIds.ORIG_TEST1);
		assert(safeCompare(person2.getOriginator(),person.getOriginator()));
		assert(safeCompare(person2.getLocalId(),person.getLocalId()));
		assert(safeCompare(person2.getLocalIdType(),person.getLocalIdType()));
		assert(safeCompare(person2.getPrimaryId(),person.getPrimaryId()));
		assert(safeCompare(person2.getPrimaryIdType(),person.getPrimaryIdType()));
		
		assert(safeCompare(person2.getGivenName(),person.getGivenName()));
		assert(safeCompare(person2.getOtherGivenNames(),person.getOtherGivenNames()));
		assert(safeCompare(person2.getSurname(),person.getSurname()));
		assert(safeCompare(person2.getTitle(),person.getTitle()));
		assert(safeCompare(person2.getPrevSurname(),person.getPrevSurname()));

		assert(safeCompare(person2.getPostcode(),person.getPostcode()));
		assert(safeCompare(person2.getStreet(),person.getStreet()));

		assert(safeCompare(person2.getGender(),person.getGender()));
		assert(safeCompare(person2.getDateOfBirth(),person.getDateOfBirth()));
		assert(safeCompare(person2.getDateOfDeath(),person.getDateOfDeath()));

	}

	@Test
	public void testStoreWithNationalId() throws MpiException {
		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_7).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		person.setPrimaryIdType("NHS").setPrimaryId("900000001");
		PersonDAO.create(conn, person);
		
		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_7, TestIds.ORIG_TEST1);
		assert(safeCompare(person2.getOriginator(),person.getOriginator()));
		assert(safeCompare(person2.getLocalId(),person.getLocalId()));
		assert(safeCompare(person2.getLocalIdType(),person.getLocalIdType()));

		assert(safeCompare(person2.getPrimaryId(),person.getPrimaryId()));
		assert(safeCompare(person2.getPrimaryIdType(),person.getPrimaryIdType()));
		
		assert(safeCompare(person2.getGivenName(),person.getGivenName()));
		assert(safeCompare(person2.getOtherGivenNames(),person.getOtherGivenNames()));
		assert(safeCompare(person2.getSurname(),person.getSurname()));
		assert(safeCompare(person2.getTitle(),person.getTitle()));
		assert(safeCompare(person2.getPrevSurname(),person.getPrevSurname()));

		assert(safeCompare(person2.getPostcode(),person.getPostcode()));
		assert(safeCompare(person2.getStreet(),person.getStreet()));

		assert(safeCompare(person2.getGender(),person.getGender()));
		assert(safeCompare(person2.getDateOfBirth(),person.getDateOfBirth()));
		assert(safeCompare(person2.getDateOfDeath(),person.getDateOfDeath()));

	}

	@Test
	public void testUpdate() throws MpiException, InterruptedException {

		Person person = new Person();
		person.setOriginator(TestIds.ORIG_TEST1).setLocalId(TestIds.TEST_LOCALID_8).setLocalIdType(TestIds.LOCAL_TYPE_MR);
		person.setTitle("MR").setGivenName("NICK").setOtherGivenNames("IOAN").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("OAKDENE, TOWNFIELD LANE");
		PersonDAO.create(conn, person);
		Timestamp originalUpdated = person.getLastUpdated();
		Thread.sleep(100); // ensure that the update time changes

		person.setGivenName("NICHOLAS");
		person.setPostcode("IA52245");
		PersonDAO.update(conn, person);
		assert(person != null);
		
		Person person2 = PersonDAO.findByLocalId(conn, TestIds.LOCAL_TYPE_MR, TestIds.TEST_LOCALID_8, TestIds.ORIG_TEST1);
		assert(safeCompare(person2.getOriginator(),person.getOriginator()));
		assert(safeCompare(person2.getLocalId(),person.getLocalId()));
		assert(safeCompare(person2.getLocalIdType(),person.getLocalIdType()));
		assert(safeCompare(person2.getPrimaryId(),person.getPrimaryId()));
		assert(safeCompare(person2.getPrimaryIdType(),person.getPrimaryIdType()));
		
		assert(safeCompare(person2.getGivenName(),person.getGivenName()));
		assert(safeCompare(person2.getOtherGivenNames(),person.getOtherGivenNames()));
		assert(safeCompare(person2.getSurname(),person.getSurname()));
		assert(safeCompare(person2.getTitle(),person.getTitle()));
		assert(safeCompare(person2.getPrevSurname(),person.getPrevSurname()));

		assert(safeCompare(person2.getPostcode(),person.getPostcode()));
		assert(safeCompare(person2.getStreet(),person.getStreet()));

		assert(safeCompare(person2.getGender(),person.getGender()));
		assert(safeCompare(person2.getDateOfBirth(),person.getDateOfBirth()));
		assert(safeCompare(person2.getDateOfDeath(),person.getDateOfDeath()));
		assert(person2.getLastUpdated().compareTo(originalUpdated)>0);
		assert(person2.getCreationDate().compareTo(originalUpdated)==0);

	}
	
	private boolean safeCompare(String a, String b) {
		boolean match = true;
		boolean nomatch = false;
		
		if (a==null) {
			if (b==null) { 
				return match;
			} else {
				return nomatch;
			}
		}
		return (a.compareTo(b)==0);
	}
	private boolean safeCompare(Date a, Date b) {
		boolean match = true;
		boolean nomatch = false;
		
		if (a==null) {
			if (b==null) { 
				return match;
			} else {
				return nomatch;
			}
		}
		return (a.compareTo(b)==0);
	}	
	private static java.util.Date getDate(String sDate){
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}

}
