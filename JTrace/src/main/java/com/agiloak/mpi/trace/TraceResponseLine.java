package com.agiloak.mpi.trace;

import java.util.Date;

public class TraceResponseLine {
	
	private long masterId;
	private Double weight;
	
	private String givenName; 
	private String otherGivenNames; 
	private String surname; 
	private String prevSurname;
	private String postcode; 
	private Date dateOfBirth; 
	private String gender; 
	private String street;
	
	// denormalisation to aid performance on UI as data has already been read.
	private String longName;
	private String longAddress;
	
	public long getMasterId() {
		return masterId;
	}
	public void setMasterId(long masterId) {
		this.masterId = masterId;
	}
	public Double getWeight() {
		return weight;
	}
	public void setWeight(Double weight) {
		this.weight = weight;
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
	public String getPostcode() {
		return postcode;
	}
	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	public String getStreet() {
		return street;
	}
	public void setStreet(String street) {
		this.street = street;
	} 
	public String getLongName() {
		return longName;
	}
	public void setLongName(String longName) {
		this.longName = longName;
	}
	public String getLongAddress() {
		return longAddress;
	}
	public void setLongAddress(String longAddress) {
		this.longAddress = longAddress;
	}
	public Date getDateOfBirth() {
		return dateOfBirth;
	}
	public void setDateOfBirth(Date dateOfBirth) {
		this.dateOfBirth = dateOfBirth;
	}

}
