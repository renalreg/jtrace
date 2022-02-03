package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public class MatchRulesTest {
	
	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1962-07-31");
	private Date d4 = getDate("1961-08-31");
	private Date d5 = getDate("1961-07-30");
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
	}
	
	@Test
	public void testGetSafeSubstring() throws MpiException {
		UKRDCIndexManager mgr = new UKRDCIndexManager();
		
		// safe string should protect against null, empty and length longer than string
		assert(mgr.getSafeSubstring(null,1).equals(""));
		assert(mgr.getSafeSubstring("",1).equals(""));
		assert(mgr.getSafeSubstring("A",5).equals("A"));

		// safe string should return correct portion of the string
		assert(mgr.getSafeSubstring("ABCDEFG",1).equals("A"));
		assert(mgr.getSafeSubstring("ABCDEFG",2).equals("AB"));
		assert(mgr.getSafeSubstring("ABCDEFG",3).equals("ABC"));
		assert(mgr.getSafeSubstring("ABCDEFG",4).equals("ABCD"));
		assert(mgr.getSafeSubstring("ABCDEFG",5).equals("ABCDE"));

	}

	@Test
	public void testMatchDOBParts() throws MpiException {

		UKRDCIndexManager mgr = new UKRDCIndexManager();

		// matchDOBParts should protect against null dates
		assert(mgr.matchDobParts(null,d1)==0);
		assert(mgr.matchDobParts(null,null)==0);
		assert(mgr.matchDobParts(d1,null)==0);

		// Happy day scenarios
		// full Match
		assert(mgr.matchDobParts(d1,d1)==3);
		// 2 part Match
		assert(mgr.matchDobParts(d1,d2)==2);
		assert(mgr.matchDobParts(d1,d3)==2);
		assert(mgr.matchDobParts(d1,d4)==2);
		// 1 part Match
		assert(mgr.matchDobParts(d2,d3)==1);
		assert(mgr.matchDobParts(d2,d4)==1);
		assert(mgr.matchDobParts(d2,d5)==1);
		assert(mgr.matchDobParts(d3,d4)==1);
		assert(mgr.matchDobParts(d3,d5)==1);
		assert(mgr.matchDobParts(d4,d5)==1);
		// no match
		assert(mgr.matchDobParts(d1,d5)==0);

	}

	@Test
	public void testVerifyMatchBasic() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();

		// Basic test 1 - person matches mr version of himself
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		MasterRecord m1 = new MasterRecord(p1);
		assert(im.verifyMatch(p1, m1));

		// Basic test 2 - person matches mr version of same data created differently
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		MasterRecord m2 = new MasterRecord().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(im.verifyMatch(p2, m2));

	}
	
	@Test
	public void testVerifyMatchPartials() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();

		// DOB Full Match
		// Surname different  = OK
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		MasterRecord m1 = new MasterRecord().setDateOfBirth(d1).setSurname("SMITH").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(im.verifyMatch(p1, m1));

		// DOB Full Match
		// Given name different  = OK
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d1).setSurname("SMITH").setGivenName("BILL").setNationalIdType("NHS").setNationalId("9000000001");
		assert(im.verifyMatch(p1, m1));

		// DOB 2-Part Match
		// Surname different 4th character = OK
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d2).setSurname("JONS").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(im.verifyMatch(p1, m1));

		// DOB 2-Part Match
		// Surname different 3rd character = NOT OK
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d2).setSurname("JOS").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(!im.verifyMatch(p1, m1));

		// DOB 2-Part Match
		// Given name different 2nd character = OK
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d2).setSurname("JONES").setGivenName("NK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(im.verifyMatch(p1, m1));

		// DOB 2-Part Match
		// Given name different 1st character = NOT OK
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d2).setSurname("JONES").setGivenName("M").setNationalIdType("NHS").setNationalId("9000000001");
		assert(!im.verifyMatch(p1, m1));

		// DOB 1-Part Match
		// Surname and given name the same = NO MATCH
		p1 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d3).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(!im.verifyMatch(p1, m1));

		// DOB 0-Part Match
		// Surname and given name the same = NO MATCH
		p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("9000000001");
		m1 = new MasterRecord().setDateOfBirth(d5).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("9000000001");
		assert(!im.verifyMatch(p1, m1));

	}	
	
	@Test
	public void testNationalIdMatch() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();

		// full match
		assert(im.nationalIdMatch(new String("ID1"), new String("ID1"), new String("NHS"), new String("NHS")));
		
		// both id's present but mismatched
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), new String("NHS"), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID1"), new String("NHS"), new String("NH2")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), new String("NHS"), new String("NH2")));

		// some empty fields
		assert(!im.nationalIdMatch(new String(""), new String("ID2"), new String("NHS"), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String(""), new String("NHS"), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), new String(""), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), new String("NHS"), new String("")));
		// some null fields
		assert(!im.nationalIdMatch(null, new String("ID2"), new String("NHS"), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), null, new String("NHS"), new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), null, new String("NHS")));
		assert(!im.nationalIdMatch(new String("ID1"), new String("ID2"), new String("NHS"), null));

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
