package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.SQLException;
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

public class LinkRecordTest {
	final static String UKRDC_TYPE = "UKRDC";
	final static String RR1 = "RR1";
	final static String RR2 = "RR2";

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
		LinkRecordDAO.deleteByPerson(conn, 1001);
		LinkRecordDAO.deleteByPerson(conn, 1002);
		LinkRecordDAO.deleteByPerson(conn, 1003);
		LinkRecordDAO.deleteByPerson(conn, 1004);
		LinkRecordDAO.deleteByPerson(conn, 1005);
		LinkRecordDAO.deleteByPerson(conn, 1006);
		LinkRecordDAO.deleteByPerson(conn, 1007);
		LinkRecordDAO.deleteByPerson(conn, 1008);
		LinkRecordDAO.deleteByPerson(conn, 1009);
		LinkRecordDAO.deleteByPerson(conn, 1010);
		MasterRecordDAO.deleteByNationalId(conn, RR1, UKRDC_TYPE);
		MasterRecordDAO.deleteByNationalId(conn, RR2, UKRDC_TYPE);
	}
		
	@Test
	public void testCreate() throws MpiException {
		int personToTest = 1009;
		LinkRecord lr = new LinkRecord(1, personToTest);
		LinkRecordDAO.create(conn,lr);
		
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);

		LinkRecord lr2 = links.get(0);
		
		verifyEqual(lr2, lr);
	}
	
	@Test
	public void testCreateAllFields() throws MpiException {
		int personToTest = 1001;
		LinkRecord lr = new LinkRecord(1,personToTest);
		lr.setUpdatedBy("Nick");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(conn, lr);
		
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);

		LinkRecord lr2 = links.get(0);
		verifyEqual(lr2, lr);

	}
	
	@Test
	public void testFind() throws MpiException {
		int personToTest = 1002;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);

		LinkRecord lr2 =LinkRecordDAO.find(conn, 1,personToTest);
		assert(lr.getId()==lr2.getId());
	}
	
	@Test
	public void testFindByPerson() throws MpiException {
		int personToTest = 1003;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		
		List<LinkRecord> links =LinkRecordDAO.findByPerson(conn, personToTest);
		assert(lr.getId()==links.get(0).getId());
		assert(links.size()==1);
	}
	
	@Test
	public void testNoFindByPerson() throws MpiException {
		int personToTest = 1004;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);
		
		List<LinkRecord> links2 =LinkRecordDAO.findByPerson(conn, 99);
		assert(links2.size()==0);
	}

	@Test
	public void testNoFind() throws MpiException {
		int personToTest = 1005;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);

		LinkRecord lr2 =LinkRecordDAO.find(conn, 2,personToTest);
		assert(lr2==null);
	}

	@Test
	public void testDelete() throws MpiException {
		int personToTest = 1006;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);

		LinkRecordDAO.delete(conn, lr);
		List<LinkRecord> links2 = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links2.size()==0);
	}
	
	@Test
	public void testDeleteByPerson() throws MpiException {
		int personToTest = 1007;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(conn, lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links.size()==1);
		
		LinkRecordDAO.deleteByPerson(conn, personToTest);
		List<LinkRecord> links2 = LinkRecordDAO.findByPerson(conn, personToTest);
		assert(links2.size()==0);
		
	}

	@Test
	public void testFindByPersonAndType() throws MpiException {
		int personToTest = 8;
		MasterRecord mr = new MasterRecord();
		mr.setNationalId(RR1).setNationalIdType(UKRDC_TYPE);
		mr.setDateOfBirth(new Date());
		mr.setEffectiveDate(new Date());
		MasterRecordDAO.create(conn, mr);
		LinkRecord lr = new LinkRecord(mr.getId(),personToTest);
		LinkRecordDAO.create(conn, lr);
		
		LinkRecord link = LinkRecordDAO.findByPersonAndType(conn, personToTest, UKRDC_TYPE);
		System.out.println("=========BEGIN TEST DEBUG=========");
		System.out.println("lr.getId()");
		System.out.println(lr.getId());
		System.out.println("link.getId()");
		System.out.println(link.getId());
		System.out.println("=========END TEST DEBUG=========");
		assert(lr.getId()==link.getId());
	}

	@Test
	public void testCountByMasterAndOriginator() throws MpiException {
		
		Person person1 = new Person();
		person1.setOriginator("TORG1").setLocalId("TST1000001").setLocalIdType("MR");
		person1.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person1.setDateOfBirth(new Date());
		person1.setGender("1");
		PersonDAO.create(conn, person1);

		Person person2 = new Person();
		person2.setOriginator("TORG1").setLocalId("TST1000002").setLocalIdType("MR");
		person2.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person2.setDateOfBirth(new Date());
		person2.setGender("1");
		PersonDAO.create(conn, person2);

		MasterRecord mr = new MasterRecord();
		mr.setNationalId(RR2).setNationalIdType(UKRDC_TYPE);
		mr.setDateOfBirth(new Date());
		mr.setEffectiveDate(new Date());
		MasterRecordDAO.create(conn, mr);
		LinkRecord lr = new LinkRecord(mr.getId(),person1.getId());
		LinkRecordDAO.create(conn, lr);
		
		int count = LinkRecordDAO.countByMasterAndOriginator(conn, mr.getId(), "TORG1");
		assert(count==1);
	
		lr = new LinkRecord(mr.getId(),person2.getId());
		LinkRecordDAO.create(conn, lr);
		
		count = LinkRecordDAO.countByMasterAndOriginator(conn, mr.getId(), "TORG1");
		assert(count==2);

	}

	private void verifyEqual(LinkRecord lr2, LinkRecord lr1) throws MpiException {
		assert(lr2.getId()==lr1.getId());
		assert(lr2.getMasterId()==lr1.getMasterId());
		assert(lr2.getPersonId()==lr1.getPersonId());
		// Note the following assertion fails if coded the other way around! looks like a bug in compareTo - beware
		assert(lr2.getLastUpdated().compareTo(lr1.getLastUpdated())==0);
		assert(lr2.getCreationDate().compareTo(lr1.getCreationDate())==0);
		assert(lr2.getLinkCode()==lr1.getLinkCode());
		assert(lr2.getLinkType()==lr1.getLinkType());
		if (lr2.getUpdatedBy()==null) {
			assert(lr2.getUpdatedBy()==null);
		} else {
			assert(lr2.getUpdatedBy().equals(lr1.getUpdatedBy()));
		}
		
	}
	
}
