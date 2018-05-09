package com.agiloak.mpi.index;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;

public class UKRDCIndexManagerValidationUnitTest3 {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private Date d1 = getDate("1962-08-31");

	@Test
	public void testDuplicateNatIdFromOrg() throws MpiException, SQLException {
		String originator = "IMVUT31";

		// setup a person
		Person p1 = new Person().setDateOfBirth(d1).setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000001").setLocalIdType("MR").setOriginator(originator);
		p1.addNationalId(new NationalIdentity("NHS", "1000000001"));
		p1.setSurname("JONES").setGivenName("NICK").setGender("M").setDateOfBirth(d1);
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.store(p1);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);

		// force duplicate error
		Person p2 = new Person().setDateOfBirth(d1).setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("00000002").setLocalIdType("MR").setOriginator(originator);
		p2.addNationalId(new NationalIdentity("NHS", "1000000001"));
		p2.setSurname("JONES").setGivenName("NICK").setGender("M").setDateOfBirth(d1);
		UKRDCIndexManagerResponse resp2 = im.validate(p2);
		assert(resp2.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp2.getMessage().contains("Another record from Unit"));
		
	}

	@Test
	public void testDuplicateNatIdFromOrgWithSkip() throws MpiException, SQLException {
		String originator = "IMVUT31";

		// setup a person
		Person p1 = new Person().setDateOfBirth(d1).setGivenName("NICHOLAS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("00000001").setLocalIdType("MR").setOriginator(originator);
		p1.addNationalId(new NationalIdentity("NHS", "1000000001"));
		p1.setSurname("JONES").setGivenName("NICK").setGender("M").setDateOfBirth(d1);
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp1 = im.store(p1);
		assert(resp1.getStatus()==UKRDCIndexManagerResponse.SUCCESS);

		// force duplicate error
		Person p2 = new Person().setDateOfBirth(d1).setGivenName("NICHOLAS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("00000002").setLocalIdType("MR").setOriginator(originator);
		p2.addNationalId(new NationalIdentity("NHS", "1000000001"));
		p2.setSurname("JONES").setGivenName("NICK").setGender("M").setDateOfBirth(d1);
		p2.setSkipDuplicateCheck(true);
		UKRDCIndexManagerResponse resp2 = im.validate(p2);
		assert(resp2.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		UKRDCIndexManagerResponse resp3 = im.store(p2);
		assert(resp3.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
		assert(resp3.getNationalIdentity().getId().equals(resp1.getNationalIdentity().getId()));
		
		
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
