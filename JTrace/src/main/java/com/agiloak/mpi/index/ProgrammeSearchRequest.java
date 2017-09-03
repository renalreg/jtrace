package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.agiloak.mpi.MpiException;

public class ProgrammeSearchRequest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

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
	public ProgrammeSearchRequest setDateOfBirth(String dateOfBirth) throws MpiException{
	    try {
		   this.dateOfBirth = formatter.parse(dateOfBirth);
		} catch (ParseException e) {
			e.printStackTrace();
			throw new MpiException("Invalid DOB Format");
		}	
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
