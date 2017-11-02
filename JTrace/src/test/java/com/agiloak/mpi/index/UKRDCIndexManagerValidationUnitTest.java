package com.agiloak.mpi.index;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;

public class UKRDCIndexManagerValidationUnitTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private Date d1 = getDate("1962-08-31");

	@Test
	public void testMissingSN() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	
	@Test
	public void testShortSN() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("J").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testMissingGN() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	
	@Test
	public void testShortGN() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}

	@Test
	public void testMissingGender() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	
	@Test
	public void testShortGender() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	
	@Test
	public void testMissingDOB() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	
	@Test
	public void testMissingLocalId() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testShortLocalId() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("").setLocalIdType("MR").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testMissingLocalIdType() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000000").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testShortLocalIdType() throws MpiException, SQLException {
		String originator = "IMUT1";
		String idBase = originator+"000";
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000000").setLocalIdType("").setOriginator(originator);
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testMissingOriginator() throws MpiException, SQLException {
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000000").setLocalIdType("MR");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
	}
	@Test
	public void testShortOriginator() throws MpiException, SQLException {
		exception.expect(MpiException.class);

		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000000").setLocalIdType("MR").setOriginator("");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
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
