package com.agiloak.mpi.workitem;

import java.util.Date;

public class WorkItem {

	public final static int STATUS_OPEN = 1;
	public final static int STATUS_WIP = 2;
	public final static int STATUS_CLOSED = 3;

	public WorkItem(String desc) {
		this.status = STATUS_OPEN;
		this.description = desc;
		this.creationTime = new Date();
	}
	
	private Date creationTime;
	private String description;
	private int status;
	
	public Date getCreationTime() {
		return creationTime;
	}
	public WorkItem setCreationTime(Date creationTime) {
		this.creationTime = creationTime;
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
