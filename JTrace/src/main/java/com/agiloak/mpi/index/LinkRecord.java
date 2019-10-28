package com.agiloak.mpi.index;

import java.sql.Timestamp;

public class LinkRecord {
	
	public static final int AUTOMATIC_TYPE = 0;
	public static final int MANUAL_TYPE = 1;

	private Timestamp lastUpdated;
	private Timestamp creationDate;

	public LinkRecord(int masterId, int personId) {
		this.masterId = masterId;
		this.personId  = personId;
	}
	
	private int id;
	private int masterId;
	private int personId;
	
	private int linkType;
	private int linkCode;
	private String linkDesc;
	private String updatedBy;
	
	public int getId() {
		return id;
	}
	public LinkRecord setId(int id) {
		this.id = id;
		return this;
	}
	public Timestamp getLastUpdated() {
		return lastUpdated;
	}
	public LinkRecord setLastUpdated(Timestamp lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}
	public Timestamp getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	public int getMasterId() {
		return masterId;
	}
	public LinkRecord setMasterId(int masterId) {
		this.masterId = masterId;
		return this;
	}
	public int getPersonId() {
		return personId;
	}
	public LinkRecord setPersonId(int personId) {
		this.personId = personId;
		return this;
	}
	public int getLinkType() {
		return linkType;
	}
	public LinkRecord setLinkType(int linkType) {
		this.linkType = linkType;
		return this;
	}
	public int getLinkCode() {
		return linkCode;
	}
	public LinkRecord setLinkCode(int linkCode) {
		this.linkCode = linkCode;
		return this;
	}
	public String getLinkDesc() {
		return linkDesc;
	}
	public LinkRecord setLinkDesc(String linkDesc) {
		this.linkDesc = linkDesc;
		return this;
	}
	public String getUpdatedBy() {
		return updatedBy;
	}
	public LinkRecord setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
		return this;
	}
	
}
