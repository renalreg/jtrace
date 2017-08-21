package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.agiloak.mpi.MpiException;

public class IndexManagerTest {
	
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
	public void testIndexManagerCreateSet() throws MpiException {
		Person person = new Person();
		person.setLocalId("100001");
		person.setLocalIdType("MR");
		person.setOriginator("MY");
		person.setGivenName("Nick");
		person.setOtherGivenNames("Ioan");
		person.setSurname("JONES");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");
		
		person.setPrimaryId("9000000001");
		person.setPrimaryIdType("NHS");
		
		IndexManager im = new IndexManager();
		im.create(person);
		assert(true);

		// test case 2
		person.setLocalId("100002");
		im.create(person);
		assert(true);
	
		//TODO - TC3 add in merge
		
		// test case 4
		person.setLocalId("1001");
		person.setOriginator("ABC");
		im.create(person);
		assert(true);

		// test case 5
		person.setLocalId("101");
		person.setOriginator("DEF");
		person.setDateOfBirth(getDate("1961-08-31"));
		im.create(person);
		assert(true);

		// test case 6
		person.setLocalId("101");
		person.setOriginator("DEF");
		person.setDateOfBirth(getDate("1960-08-31"));
		im.update(person);
		assert(true);

		// test case 7
		person.setLocalId("101");
		person.setOriginator("DEF");
		person.setPrimaryId("9000000002");
		im.update(person);
		assert(true);

		// test case 8
		person.setLocalId("102");
		person.setOriginator("DEF");
		person.setPrimaryId(null);
		person.setPrimaryIdType(null);
		person.setDateOfBirth(getDate("1963-08-31"));
		im.create(person);
		assert(true);

		// test case 9 - add 2ndary ref
		person.setPrimaryId("9990001");
		person.setPrimaryIdType("UKRDC");
		im.update(person);
		assert(true);

		// test case 10 - delete 2ndary ref
		person.setPrimaryId(null);
		person.setPrimaryIdType(null);
		im.update(person);
		assert(true);

	}

	//@Test
	public void testIndexManagerCreate() throws MpiException {
		Person person = new Person();
		person.setLocalId("TST1000009");
		person.setLocalIdType("MR");
		person.setOriginator("MY");
		person.setGivenName("Nick");
		person.setOtherGivenNames("Ioan");
		person.setSurname("JONES");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");
		
		person.setPrimaryId("9000000001");
		person.setPrimaryIdType("NHS");
		
		IndexManager im = new IndexManager();
		im.create(person);
		assert(true);

		// insert duplicate
		im.create(person);
		assert(true);
	}

	//@Test
	public void testIndexManagerUpdate() throws MpiException {
		
		IndexManager im = new IndexManager();
		Person person = new Person();
		
		person.setLocalId("TST1000001");
		person.setLocalIdType("MR");
		person.setOriginator("MY");
		person.setGivenName("Nick");
		person.setOtherGivenNames("Ioan");
		person.setSurname("JONES");
		person.setTitle("MR");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setDateOfDeath(getDate("2062-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");

		im.create(person);
	
		// update person
		person.setSurname("JONES3");
		im.update(person);
		assert(true);
	}

}
