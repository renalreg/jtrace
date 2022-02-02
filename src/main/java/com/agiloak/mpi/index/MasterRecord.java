package com.agiloak.mpi.index;

import java.sql.Timestamp;
import java.util.Date;

public class MasterRecord {

	public static final int OK = 0;
	public static final int INVESTIGATE = 1;
	
	private Timestamp lastUpdated;
	private Timestamp creationDate;
	private int id;
	private String nationalId;
	private String nationalIdType;
	private Date dateOfBirth; 
	private String gender; 
	private String givenName; 
	private String surname; 
	private int status;
	private Date effectiveDate;

	public MasterRecord() {
	}
	public MasterRecord(Person person) {
		this.dateOfBirth = person.getDateOfBirth();
		this.gender=person.getGender();
		this.givenName=person.getGivenName();
		this.surname=person.getSurname();
		this.nationalId=person.getPrimaryId();
		this.nationalIdType=person.getPrimaryIdType();
		this.effectiveDate=person.getEffectiveDate();
	}
	
	public void updateDemographics(Person person) {
		this.dateOfBirth = person.getDateOfBirth();
		this.gender=person.getGender();
		this.givenName=person.getGivenName();
		this.surname=person.getSurname();
		this.effectiveDate=person.getEffectiveDate();
	}

	public Timestamp getLastUpdated() {
		return lastUpdated;
	}
	public MasterRecord setLastUpdated(Timestamp lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}

	public Timestamp getCreationDate() {
		return creationDate;
	}
	public MasterRecord setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
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
		if (nationalId!=null) {
			this.nationalId = nationalId.trim();
		} else {
			this.nationalId = null;
		}
		return this;
	}

	public String getNationalIdType() {
		return nationalIdType;
	}

	public MasterRecord setNationalIdType(String nationalIdType) {
		if (nationalIdType!=null) {
			this.nationalIdType = nationalIdType.trim();
		} else {
			this.nationalIdType = null;
		}
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
	public int getStatus() {
		return status;
	}
	public MasterRecord setStatus(int status) {
		this.status = status;
		return this;
	}
	public Date getEffectiveDate() {
		return effectiveDate;
	}
	public MasterRecord setEffectiveDate(Date effectiveDate) {
		this.effectiveDate = effectiveDate;
		return this;
	}
	public NationalIdentity getNationalIdentity() {
		return new NationalIdentity(nationalIdType, nationalId);
	}
	
}
