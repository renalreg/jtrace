package com.agiloak.mpi.audit;

import java.util.Date;

public class Audit {

	public final static int NO_MATCH_ASSIGN_NEW = 1;
	public final static int NEW_MATCH_THROUGH_NATIONAL_ID = 2;
	public final static int UKRDC_MERGE = 3;

	public Audit(int type, int personId, int masterId, String desc) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.lastUpdated = new Date();
	}
	
	private int id;
	private Date lastUpdated;
	private String description;
	private int personId;
	private int masterId;
	private int type;
	private String updatedBy;
	
	public int getId() {
		return id;
	}
	public Audit setId(int id) {
		this.id = id;
		return this;
	}
	public int getPersonId() {
		return personId;
	}
	public Audit setPersonId(int personId) {
		this.personId = personId;
		return this;
	}
	public int getMasterId() {
		return masterId;
	}
	public Audit setMasterId(int masterId) {
		this.masterId = masterId;
		return this;
	}
	public int getType() {
		return type;
	}
	public Audit setType(int type) {
		this.type = type;
		return this;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public Audit setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}
	public String getDescription() {
		return description;
	}
	public Audit setDescription(String description) {
		this.description = description;
		return this;
	}
	public String getUpdatedBy() {
		return updatedBy;
	}
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	
}
