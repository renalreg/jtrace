package com.agiloak.mpi.index;

import java.util.Date;

public class Person {

	private int id;
	private String localId;
	private String localIdType;
	private String originator;

	private String nationalId;
	private String nationalIdType;

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
		this.localId = localId;
		return this;
	}

	public String getLocalIdType() {
		return localIdType;
	}

	public Person setLocalIdType(String localIdType) {
		this.localIdType = localIdType;
		return this;
	}
	
	public String getOriginator() {
		return originator;
	}


	public Person setOriginator(String originator) {
		this.originator = originator;
		return this;
	}

	public String getGender() {
		return gender;
	}

	public Person setGender(String gender) {
		this.gender = gender;
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
		this.postcode = postcode;
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

	public String getNationalId() {
		return nationalId;
	}

	public Person setNationalId(String nationalId) {
		this.nationalId = nationalId;
		return this;
	}

	public String getNationalIdType() {
		return nationalIdType;
	}

	public Person setNationalIdType(String nationalIdType) {
		this.nationalIdType = nationalIdType;
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

}
