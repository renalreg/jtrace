package com.agiloak.mpi.index;

import java.util.Date;

public class LinkRecord {

	public LinkRecord(int masterId, int personId) {
		this.masterId = masterId;
		this.personId  = personId;
		this.lastUpdated = new Date();
	}
	
	private int id;
	private Date lastUpdated;
	private int masterId;
	private int personId;
	
	private int linkType;
	private int linkCode;
	private String updatedBy;
	
	public int getId() {
		return id;
	}
	public LinkRecord setId(int id) {
		this.id = id;
		return this;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public LinkRecord setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
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
	public String getUpdatedBy() {
		return updatedBy;
	}
	public LinkRecord setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
		return this;
	}
	
}
