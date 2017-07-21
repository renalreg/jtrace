package com.agiloak.mpi.index;

import java.util.Date;

public class MasterRecord {

	private Date lastUpdated;
	private int id;
	private String nationalId;
	private Date dateOfBirth; 
	private String gender; 
	private String givenName; 
	private String surname; 

	public MasterRecord() {
		this.lastUpdated = new Date();
	}
	
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public MasterRecord setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}

	public int getId() {
		return id;
	}

	public MasterRecord setId(int id) {
		this.id = id;
		return this;
	}

	public String getNationalId() {
		return nationalId;
	}

	public MasterRecord setNationalId(String nationalId) {
		this.nationalId = nationalId;
		return this;
	}

	public Date getDateOfBirth() {
		return dateOfBirth;
	}

	public MasterRecord setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
		return this;
	}

	public String getGender() {
		return gender;
	}

	public MasterRecord setGender(String gender) {
		this.gender = gender;
		return this;
	}

	public String getGivenName() {
		return givenName;
	}

	public MasterRecord setGivenName(String givenName) {
		this.givenName = givenName;
		return this;
	}

	public String getSurname() {
		return surname;
	}

	public MasterRecord setSurname(String surname) {
		this.surname = surname;
		return this;
	}
	
}
