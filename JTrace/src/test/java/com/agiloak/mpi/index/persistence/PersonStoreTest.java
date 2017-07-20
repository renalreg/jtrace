package com.agiloak.mpi.index.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.agiloak.mpi.index.Person;

public class PersonStoreTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
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
	

	@Test
	public void testStore() {
		Person person = new Person();
		person.setLocalId("TST1000004");
		person.setLocalIdType("MR");
		person.setLocalIdOriginator("PST");
		person.setGivenName("Nick");
		person.setOtherGivenNames("Ioan");
		person.setSurname("JONES");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");
		
		PersonDAO.insert(person);
		assert(true);
	}

	@Test
	public void testStore2() {
		Person person = new Person();
		person.setLocalId("TST1000005");
		person.setLocalIdType("MR");
		person.setLocalIdOriginator("PST");
		person.setGivenName("Nick");
		person.setOtherGivenNames("Ioan");
		person.setSurname("JONES");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");
		
		PersonDAO.insert(person);
		assert(true);
	}

	@Test
	public void testUpdate() {

		Person person = PersonDAO.findPerson("MR", "TST1000005", "PST");
		assert(person != null);

		person.setLocalId("TST1000005");
		person.setLocalIdType("MR");
		person.setLocalIdOriginator("MY");
		person.setGivenName("Nick2");
		person.setOtherGivenNames("Ioan2");
		person.setSurname("JONES2s");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");

		PersonDAO.update(person);
		assert(true);
	}
}
