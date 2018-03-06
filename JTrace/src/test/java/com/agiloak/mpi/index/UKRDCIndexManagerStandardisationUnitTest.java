package com.agiloak.mpi.index;

import org.junit.Test;

import com.agiloak.mpi.MpiException;

public class UKRDCIndexManagerStandardisationUnitTest {
	
	@Test
	public void testHappyDay1() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ").setOtherGivenNames(" i T  ").setTitle(" mR ");
		p1.setPostcode(" ch1  6lb ").setStreet(" 1 townField LaNe ");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getGivenName().equals("NICHOLAS"));
		assert(p1.getOtherGivenNames().equals("I T"));
		assert(p1.getSurname().equals("JONES"));
		assert(p1.getTitle().equals("MR"));
		assert(p1.getGender().equals("M"));
		assert(p1.getPostcode().equals("CH1 6LB"));
		assert(p1.getStreet().equals("1 TOWNFIELD LANE"));
	}
	
	@Test
	public void testPostcodeUnformatted5() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("c16lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("C1 6LB"));
	}
	
	@Test
	public void testPostcodeUnformatted6() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("ch16lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("CH1 6LB"));
	}
	
	@Test
	public void testPostcodeUnformatted7() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("ch116lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("CH11 6LB"));
	}
	
	@Test
	public void testPostcodeBadFormat5() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("c 16lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("C1 6LB"));
	}
	
	@Test
	public void testPostcodeBadFormat6() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("c h16lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("CH1 6LB"));
	}
	
	@Test
	public void testPostcodeBadFormat7() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode("c h116lb");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("CH11 6LB"));
	}
	
	@Test
	public void testShortPostcode() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode(" c1  lb ");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("C1LB"));
	}
	@Test
	public void testLongPostcode() throws MpiException {
		Person p1 = new Person().setGivenName(" nicholas ").setGender(" m ").setSurname(" jones ");
		p1.setPostcode(" ch11  66lb ");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.standardise(p1);
		assert(p1.getPostcode().equals("CH1166LB"));
	}
}
