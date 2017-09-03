package com.agiloak.mpi.index;

import java.util.Date;

public class ProgrammeSearchRequest {

	private String givenName; 
	private String surname; 
	private String gender; 
	private Date dateOfBirth; 
	private NationalIdentity nationalId;
	
	public String getGivenName() {
		return givenName;
	}
	public ProgrammeSearchRequest setGivenName(String givenName) {
		this.givenName = givenName;
		return this;
	}
	public String getSurname() {
		return surname;
	}
	public ProgrammeSearchRequest setSurname(String surname) {
		this.surname = surname;
		return this;
	}
	public String getGender() {
		return gender;
	}
	public ProgrammeSearchRequest setGender(String gender) {
		this.gender = gender;
		return this;
	}
	public Date getDateOfBirth() {
		return dateOfBirth;
	}
	public ProgrammeSearchRequest setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
		return this;
	}
	public NationalIdentity getNationalId() {
		return nationalId;
	}
	public ProgrammeSearchRequest setNationalId(NationalIdentity nationalId) {
		this.nationalId = nationalId;
		return this;
	}

}
