package com.agiloak.mpi.index;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Person {

	private int id;
	private String localId;
	private String localIdType;
	private String originator;

	private String primaryId;
	private String primaryIdType;

	private List<NationalIdentity> nationalIds;
	private Date dateOfBirth; 
	private String gender; 
	private Date dateOfDeath; 

	private String givenName; 
	private String otherGivenNames; 
	private String surname; 
	private String prevSurname;
	private String title;

	private String postcode; 
	private String street;

	// Standardised fields
	private String stdSurname;
	private String stdGivenName;
	private String stdPrevSurname;
	private String stdPostcode;
	
	public Person(){
		this.nationalIds = new ArrayList<NationalIdentity>();
	}

	public int getId() {
		return id;
	}

	public Person setId(int id) {
		this.id = id;
		return this;
	}

	public String getLocalId() {
		return localId;
	}

	public Person setLocalId(String localId) {
		if (localId!=null) {
			this.localId = localId.trim();
		} else {
			this.localId = null;
		}
		return this;
	}

	public String getLocalIdType() {
		return localIdType;
	}

	public Person setLocalIdType(String localIdType) {
		if (localIdType!=null) {
			this.localIdType = localIdType.trim();
		} else {
			this.localIdType = null;
		}
		return this;
	}
	
	public String getOriginator() {
		return originator;
	}


	public Person setOriginator(String originator) {
		if (originator!=null) {
			this.originator = originator.trim();
		} else {
			this.originator = null;
		}
		return this;
	}

	public String getGender() {
		return gender;
	}

	public Person setGender(String gender) {
		if (gender!=null) {
			this.gender = gender.trim();
		} else {
			this.gender = null;
		}
		return this;
	}

	public String getGivenName() {
		return givenName;
	}

	public Person setGivenName(String givenName) {
		this.givenName = givenName;
		return this;
	}

	public String getOtherGivenNames() {
		return otherGivenNames;
	}

	public Person setOtherGivenNames(String otherGivenNames) {
		this.otherGivenNames = otherGivenNames;
		return this;
	}

	public String getSurname() {
		return surname;
	}

	public Person setSurname(String surname) {
		this.surname = surname;
		return this;
	}

	public String getPrevSurname() {
		return prevSurname;
	}

	public Person setPrevSurname(String prevSurname) {
		this.prevSurname = prevSurname;
		return this;
	}

	public String getTitle() {
		return title;
	}

	public Person setTitle(String title) {
		this.title = title;
		return this;
	}

	public String getPostcode() {
		return postcode;
	}

	public Person setPostcode(String postcode) {
		if (postcode!=null) {
			this.postcode = postcode.trim();
		} else {
			this.postcode = null;
		}
		return this;
	}

	public String getStreet() {
		return street;
	}

	public Person setStreet(String street) {
		this.street = street;
		return this;
	}
	
	public String getStdSurname() {
		return stdSurname;
	}

	public Person setStdSurname(String stdSurname) {
		this.stdSurname = stdSurname;
		return this;
	}

	public String getStdGivenName() {
		return stdGivenName;
	}

	public Person setStdGivenName(String stdGivenName) {
		this.stdGivenName = stdGivenName;
		return this;
	}

	public String getStdPrevSurname() {
		return stdPrevSurname;
	}

	public Person setStdPrevSurname(String stdPrevSurname) {
		this.stdPrevSurname = stdPrevSurname;
		return this;
	}

	public String getStdPostcode() {
		return stdPostcode;
	}

	public Person setStdPostcode(String stdPostcode) {
		this.stdPostcode = stdPostcode;
		return this;
	}

	public String getPrimaryId() {
		return primaryId;
	}

	public Person setPrimaryId(String primaryId) {
		if (primaryId!=null) {
			this.primaryId = primaryId.trim();
		} else {
			this.primaryId = primaryId;
		}
		return this;
	}

	public String getPrimaryIdType() {
		return primaryIdType;
	}

	public Person setPrimaryIdType(String primaryIdType) {
		if (primaryIdType!=null) {
			this.primaryIdType = primaryIdType.trim();
		} else {
			this.primaryIdType = null;
		}
		return this;
	}
	
	public List<NationalIdentity> getNationalIds() {
		return nationalIds;
	}

	public Person setNationalIds(List<NationalIdentity> nationalIds) {
		this.nationalIds = nationalIds;
		return this;
	}
	public Person addNationalId(NationalIdentity nationalId) {
		nationalIds.add(nationalId);
		return this;
	}

	public Date getDateOfBirth() {
		return dateOfBirth;
	}

	public Person setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
		return this;
	}

	public Date getDateOfDeath() {
		return dateOfDeath;
	}

	public Person setDateOfDeath(Date dateOfDeath) {
		this.dateOfDeath = dateOfDeath;
		return this;
	}
	
	public String toString() {
		return "[LID:"+originator+"-"+localIdType+"-"+localId+"]"+
			   "[NID:"+primaryIdType+"-"+primaryId+"]";
	}

}
