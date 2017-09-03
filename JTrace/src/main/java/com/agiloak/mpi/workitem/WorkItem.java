package com.agiloak.mpi.workitem;

import java.util.Date;

public class WorkItem {

	public final static int STATUS_OPEN = 1;
	public final static int STATUS_WIP = 2;
	public final static int STATUS_CLOSED = 3;

	public final static int TYPE_INVESTIGATE_DUE_TO_CHANGED_DEMOG = 1;
	public final static int TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED = 2;
	public final static int TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID = 3;
	public final static int TYPE_DEMOGS_NEAR_MATCH = 4;

	public WorkItem(int type, int personId, String desc) {
		this.type = type;
		this.personId = personId;
		this.description = desc;

		this.status = STATUS_OPEN;
		this.lastUpdated = new Date();
	}
	
	private int id;
	private Date lastUpdated;
	private String description;
	private int status;
	private int personId;
	private int type;
	
	public int getId() {
		return id;
	}
	public WorkItem setId(int id) {
		this.id = id;
		return this;
	}
	public int getPersonId() {
		return personId;
	}
	public WorkItem setPersonId(int personId) {
		this.personId = personId;
		return this;
	}
	public int getType() {
		return type;
	}
	public WorkItem setType(int type) {
		this.type = type;
		return this;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public WorkItem setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}
	public String getDescription() {
		return description;
	}
	public WorkItem setDescription(String description) {
		this.description = description;
		return this;
	}
	public int getStatus() {
		return status;
	}
	public WorkItem setStatus(int status) {
		this.status = status;
		return this;
	}
	
}
