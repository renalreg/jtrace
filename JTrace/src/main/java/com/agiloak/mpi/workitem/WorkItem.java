package com.agiloak.mpi.workitem;

import java.util.Date;

public class WorkItem {

	/**
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 */
	public WorkItem(int type, int personId, int masterId, String desc) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.status = WorkItemStatus.STATUS_OPEN;
		this.lastUpdated = new Date();
	}
	
	private int id;
	private Date lastUpdated;
	private String description;
	private int status;
	private int personId;
	private int masterId;
	private int type;
	private String updatedBy;
	private String updateDesc;
	
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
	public int getMasterId() {
		return masterId;
	}
	public WorkItem setMasterId(int masterId) {
		this.masterId = masterId;
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
	public String getUpdatedBy() {
		return updatedBy;
	}
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	public String getUpdateDesc() {
		return updateDesc;
	}
	public void setUpdateDesc(String updateDesc) {
		this.updateDesc = updateDesc;
	}

	
}
