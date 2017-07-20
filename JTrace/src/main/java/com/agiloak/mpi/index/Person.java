package com.agiloak.mpi.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Person {

	private long masterId;
	private String localId;
	private String localIdType;
	private String localIdOriginator;

	//TODO - make this a list
	private String secondaryId;
	private String secondaryIdType;
	private String secondaryIdOriginator;

	private Date dateOfBirth; 
	private String gender; 
	private Date dateOfDeath; 

	// Only fields required for search and display held in this class [POC]
	private String givenName; 
	private String otherGivenNames; 
	private String surname; 
	private String prevSurname;
	private String title;
	private List<Name> names;

	// Only fields required for search required separately [POC]
	private String postcode; 
	private String street;
	private List<Address> addresses;

	private List<ContactDetail> contactDetails;

	// Standardised fields
	private String stdSurname;
	private String stdGivenName;
	private String stdPrevSurname;
	private String stdPostcode;
	
	public Person(){
		this.names = new ArrayList<Name>();
		this.addresses = new ArrayList<Address>();
		this.contactDetails = new ArrayList<ContactDetail>();
	}
	

	public long getMasterId() {
		return masterId;
	}

	public void setMasterId(long masterId) {
		this.masterId = masterId;
	}

	public String getLocalId() {
		return localId;
	}

	public void setLocalId(String localId) {
		this.localId = localId;
	}

	public String getLocalIdType() {
		return localIdType;
	}

	public void setLocalIdType(String localIdType) {
		this.localIdType = localIdType;
	}
	
	public String getLocalIdOriginator() {
		return localIdOriginator;
	}


	public void setLocalIdOriginator(String localIdOriginator) {
		this.localIdOriginator = localIdOriginator;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getGivenName() {
		return givenName;
	}

	public void setGivenName(String givenName) {
		this.givenName = givenName;
	}

	public String getOtherGivenNames() {
		return otherGivenNames;
	}

	public void setOtherGivenNames(String otherGivenNames) {
		this.otherGivenNames = otherGivenNames;
	}

	public String getSurname() {
		return surname;
	}

	public void setSurname(String surname) {
		this.surname = surname;
	}

	public String getPrevSurname() {
		return prevSurname;
	}

	public void setPrevSurname(String prevSurname) {
		this.prevSurname = prevSurname;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getPostcode() {
		return postcode;
	}

	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}
	
	public List<Name> getNames() {
		return names;
	}

	public void addName(Name name) {
		this.names.add(name);
	}


	public List<Address> getAddresses() {
		return addresses;
	}

	public void addAddress(Address address) {
		this.addresses.add(address);
	}

	public List<ContactDetail> getContactDetails() {
		return contactDetails;
	}

	public void addContactDetail(ContactDetail contactDetail) {
		this.contactDetails.add(contactDetail);
	}

	public String getStdSurname() {
		return stdSurname;
	}


	public void setStdSurname(String stdSurname) {
		this.stdSurname = stdSurname;
	}


	public String getStdGivenName() {
		return stdGivenName;
	}


	public void setStdGivenName(String stdGivenName) {
		this.stdGivenName = stdGivenName;
	}


	public String getStdPrevSurname() {
		return stdPrevSurname;
	}


	public void setStdPrevSurname(String stdPrevSurname) {
		this.stdPrevSurname = stdPrevSurname;
	}


	public String getStdPostcode() {
		return stdPostcode;
	}


	public void setStdPostcode(String stdPostcode) {
		this.stdPostcode = stdPostcode;
	}


	public String getSecondaryId() {
		return secondaryId;
	}


	public void setSecondaryId(String secondaryId) {
		this.secondaryId = secondaryId;
	}


	public String getSecondaryIdType() {
		return secondaryIdType;
	}


	public void setSecondaryIdType(String secondaryIdType) {
		this.secondaryIdType = secondaryIdType;
	}

	public String getSecondaryIdOriginator() {
		return secondaryIdOriginator;
	}


	public void setSecondaryIdOriginator(String secondaryIdOriginator) {
		this.secondaryIdOriginator = secondaryIdOriginator;
	}


	public Date getDateOfBirth() {
		return dateOfBirth;
	}


	public void setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}


	public Date getDateOfDeath() {
		return dateOfDeath;
	}


	public void setDateOfDeath(Date dateOfDeath) {
		this.dateOfDeath = dateOfDeath;
	}



	
}
