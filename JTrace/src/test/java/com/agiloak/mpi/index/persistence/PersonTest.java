package com.agiloak.mpi.index.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class PersonTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		
		int masterId = 0;
		MasterRecord mr = MasterRecordDAO.findByNationalId("NHS0000001","NHS");
		if (mr !=null) {
			masterId = mr.getId();
			MasterRecordDAO.deleteByNationalId("NHS0000001","NHS");
		}
		
		Person person = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
		if (person != null) {
			PersonDAO.delete(person);
			if (mr !=null) {
				LinkRecord lr = LinkRecordDAO.find(masterId, person.getId());
				if (lr != null) {
					LinkRecordDAO.delete(lr);
				}
			}
		}
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000002", "TST");
		if (person2 != null) {
			PersonDAO.delete(person2);
			if (mr !=null) {
				LinkRecord lr = LinkRecordDAO.find(masterId, person2.getId());
				if (lr != null) {
					LinkRecordDAO.delete(lr);
				}
			}
		}
	}

	@Test
	public void testFindByMasterId() throws MpiException {
		
		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setPrimaryIdType("NHS").setPrimaryId("NHS0000001");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);
		Person person2 = new Person();
		person2.setOriginator("TST").setLocalId("TST1000002").setLocalIdType("MR");
		person2.setPrimaryIdType("NHS").setPrimaryId("NHS0000001");
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setDateOfDeath(getDate("2062-08-31"));
		person2.setGender("1");
		person2.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person2);
		assert(true);

		MasterRecord master = new MasterRecord(person);
		master.setEffectiveDate(new Date());
		MasterRecordDAO.create(master);
		LinkRecord l1 = new LinkRecord(master.getId(), person.getId());
		LinkRecord l2 = new LinkRecord(master.getId(), person2.getId());
		LinkRecordDAO.create(l1);
		LinkRecordDAO.create(l2);
		
		List<Person> personList = PersonDAO.findByMasterId(master.getId());
		assert(personList != null);
		assert(personList.size()==2);
		
	}

	@Test
	public void testRead() throws MpiException {
		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);
		
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
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
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);
		
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "ANO");
		assert(person2 == null);
	}

	@Test
	public void testDelete() throws MpiException {
		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);
		PersonDAO.delete(person);
		assert(true);
	}

	@Test
	public void testStore() throws MpiException {
		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);

		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
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
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		person.setPrimaryIdType("NHS").setPrimaryId("900000001");
		
		PersonDAO.create(person);
		assert(true);
		
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
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
	public void testUpdate() throws MpiException {

		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("NICK").setOtherGivenNames("IOAN").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("OAKDENE, TOWNFIELD LANE");
		PersonDAO.create(person);
		assert(person != null);

		person.setGivenName("NICHOLAS");
		person.setPostcode("IA52245");
		PersonDAO.update(person);
		assert(person != null);
		
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
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
