package com.agiloak.mpi.index.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class PersonTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup()  throws MpiException {
		Person person = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
		if (person != null) {
			PersonDAO.delete(person);
		}
		Person person2 = PersonDAO.findByLocalId("MR", "TST1000002", "TST");
		if (person2 != null) {
			PersonDAO.delete(person2);
		}
	}

	@Test
	public void testFindByMasterId() throws MpiException {
		
		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000003").setLocalIdType("MR");
		person.setNationalIdType("NHS").setNationalId("NHS0000001");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(true);
		Person person2 = new Person();
		person2.setOriginator("TST").setLocalId("TST1000004").setLocalIdType("MR");
		person2.setNationalIdType("NHS").setNationalId("NHS0000001");
		person2.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person2.setDateOfBirth(getDate("1962-08-31"));
		person2.setDateOfDeath(getDate("2062-08-31"));
		person2.setGender("1");
		person2.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person2);
		assert(true);

		MasterRecord master = new MasterRecord(person);
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
		
		// TODO: extend to check all fields
		assert(person2.getOriginator().equals("TST"));
		assert(person2.getLocalId().equals("TST1000001"));
		assert(person2.getLocalIdType().trim().equals("MR"));
		assert(person2.getGivenName().equals("Nick"));
		assert(person2.getPostcode().trim().equals("CH1 6LB"));
		
		assert(person2.getDateOfBirth().compareTo(person.getDateOfBirth())==0);
		assert(person2.getDateOfDeath().compareTo(person.getDateOfDeath())==0);
		
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
		person.setNationalIdType("NHS").setNationalId("900000001");
		
		PersonDAO.create(person);
		assert(true);
	}

	@Test
	public void testUpdate() throws MpiException {

		Person person = new Person();
		person.setOriginator("TST").setLocalId("TST1000001").setLocalIdType("MR");
		person.setTitle("MR").setGivenName("Nick").setOtherGivenNames("Ioan").setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB").setStreet("Oakdene, Townfield Lane");
		PersonDAO.create(person);
		assert(person != null);

		person.setGivenName("Nicholas");
		person.setPostcode("IA52245");
		PersonDAO.update(person);
		assert(person != null);
		
		// TODO : Extend to all fields

		Person person2 = PersonDAO.findByLocalId("MR", "TST1000001", "TST");
		assert(person2.getGivenName().equals("Nicholas"));
		assert(person2.getPostcode().trim().equals("IA52245"));

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
