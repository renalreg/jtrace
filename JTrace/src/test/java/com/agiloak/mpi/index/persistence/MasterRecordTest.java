package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class MasterRecordTest extends JTraceTest {
	
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
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000001","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000002","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000003","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000004","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000005","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000006","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000007","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000008","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000009","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000010","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000011","NHS");
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000012","NHS");
	}
	
	@Test
	public void testGetSequence() throws MpiException {
		String ukrdcId = MasterRecordDAO.allocate(conn);
		assert(ukrdcId!=null);
		assert(ukrdcId.length()==9);
	}

	@Test
	public void testDelete() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000011").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000011","NHS");
		assert(mr2!=null);
		MasterRecordDAO.delete(conn, mr);
		MasterRecord mr3 = MasterRecordDAO.findByNationalId(conn, "NHS0000011","NHS");
		assert(mr3==null);
		MasterRecord mr4 = MasterRecordDAO.get(conn, mr.getId());
		assert(mr4==null);
	}

	@Test
	public void testDeleteById() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000012").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000012","NHS");
		assert(mr2!=null);
		MasterRecordDAO.delete(conn, mr.getId());
		MasterRecord mr3 = MasterRecordDAO.findByNationalId(conn, "NHS0000012","NHS");
		assert(mr3==null);
		MasterRecord mr4 = MasterRecordDAO.get(conn, mr.getId());
		assert(mr4==null);
	}

	@Test
	public void testDeleteByNationalId() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000004").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000004","NHS");
		assert(mr2.getId()==(mr.getId()));
		MasterRecordDAO.deleteByNationalId(conn, "NHS0000004","NHS");
		assert(true);
		MasterRecord mr3 = MasterRecordDAO.findByNationalId(conn, "NHS0000004","NHS");
		assert(mr3==null);
	}
	@Test
	public void testCreate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000001").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(mr.getId()>0);
	}

	@Test
	public void testGet() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000009").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.get(conn, mr.getId());
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
		
	}

	@Test
	public void testUpdate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000005").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		mr.setGender("F");
		mr.setGivenName("Nicholas").setSurname("James");
		mr.setEffectiveDate(getDate("2017-08-24"));
		MasterRecordDAO.update(conn, mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000005","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	
	}

	@Test
	public void testUpdateInvestigate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000010").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		mr.setStatus(MasterRecord.INVESTIGATE);
		MasterRecordDAO.update(conn, mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000010","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.INVESTIGATE);
	
	}
	
	@Test
	public void testUpdateNoNationalId() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setEffectiveDate(getDate("2017-08-22"));
		exception.expect(MpiException.class);
		MasterRecordDAO.create(conn, mr);
		assert(true);
	}

	@Test
	public void testCreateDupl() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000002").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		// Second insert should fail
		exception.expect(MpiException.class);
		MasterRecordDAO.create(conn, mr);
	}
	
	@Test
	public void testFind() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000003").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, "NHS0000003","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	}

	@Test
	public void testFindByDemographics1() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BILL").setSurname("SMITH");
		mr.setNationalId("NHS0000006").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(getDate("1962-08-31")).setGivenName("BILL").setSurname("SMITH");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(conn, person);
		assert(mrl.size()==1);
		
		MasterRecord mr2 = mrl.get(0);
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	}
	
	@Test
	public void testFindByDemographics2() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BRIAN").setSurname("MAY");
		mr.setNationalId("NHS0000007").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BRIAN").setSurname("MAY");
		mr.setNationalId("NHS0000008").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(conn, mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(getDate("1962-08-31")).setGivenName("BRIAN").setSurname("MAY");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(conn, person);
		assert(mrl.size()==2);
		for (MasterRecord mr2 : mrl) {
			assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
			assert(mr2.getGivenName().equals(mr.getGivenName()));
			assert(mr2.getSurname().equals(mr.getSurname()));
			assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
			assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
			assert(mr2.getStatus()==mr.getStatus());
			assert(mr2.getStatus()==MasterRecord.OK);
		}

	}
}
