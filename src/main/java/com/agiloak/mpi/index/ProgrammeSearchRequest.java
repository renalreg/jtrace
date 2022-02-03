package com.agiloak.mpi.index;

public class ProgrammeSearchRequest {

	private String givenName; 
	private String surname; 
	private String gender; 
	private String dateOfBirth; 
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
	public String getDateOfBirth() {
		return dateOfBirth;
	}
	public ProgrammeSearchRequest setDateOfBirth(String dateOfBirth) {
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
	public ProgrammeSearchRequest setNationalId(String natIdType, String natId) {
		this.nationalId = new NationalIdentity(natIdType, natId);
		return this;
	}

}
