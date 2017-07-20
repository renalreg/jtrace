package com.agiloak.mpi.trace;

import java.util.Date;
import java.util.UUID;

import com.agiloak.mpi.index.Person;

public class TraceRequest {

	private String traceId; 
	private String traceType; 
	private String nameSwap; 

	private String localId; 
	private String localIdType; 
	private String localIdOriginator;
	
	private String givenName; 
	private String otherGivenNames; 
	private String surname; 
	private String gender; 
	private String postcode; 
	private Date dateOfBirthStart; 
	private Date dateOfBirthEnd; 
	private String street;
	
	private String longName; 
	private String longAddress;
	
	public TraceRequest() {
		//TODO : Traceid is constructed from localid and tracetype in QT 
		//       That means that a retrace requires a db clean up to stop growth - probably need to do this ....
		UUID uuid = UUID.randomUUID();
		this.traceId = uuid.toString();
	}
	
	public TraceRequest(Person person) {
		UUID uuid = UUID.randomUUID();
		this.traceId = uuid.toString();
		this.localId=person.getLocalId();
		this.localIdType=person.getLocalIdType();
		this.localIdOriginator=person.getLocalIdOriginator();
		this.givenName=person.getGivenName();
		this.otherGivenNames=person.getOtherGivenNames();
		this.surname=person.getSurname();
		this.gender=person.getGender();
		this.postcode=person.getPostcode();
		this.setDateOfBirthStart(person.getDateOfBirth());
		this.street=person.getStreet();
	}
	
	public String getTraceId() {
		return traceId;
	}
	public void setTraceId(String traceId) {
		this.traceId = traceId;
	}
	public String getTraceType() {
		return traceType;
	}
	public void setTraceType(String traceType) {
		this.traceType = traceType;
	}
	public String getNameSwap() {
		return nameSwap;
	}
	public void setNameSwap(String nameSwap) {
		this.nameSwap = nameSwap;
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
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
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

	public Date getDateOfBirthStart() {
		return dateOfBirthStart;
	}

	public void setDateOfBirthStart(Date dateOfBirthStart) {
		this.dateOfBirthStart = dateOfBirthStart;
	}

	public Date getDateOfBirthEnd() {
		return dateOfBirthEnd;
	}

	public void setDateOfBirthEnd(Date dateOfBirthEnd) {
		this.dateOfBirthEnd = dateOfBirthEnd;
	}
	
}
