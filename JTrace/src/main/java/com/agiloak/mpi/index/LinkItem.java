package com.agiloak.mpi.index;

import java.util.Date;

public class LinkItem {

	public LinkItem(int masterId, int localId) {
		this.masterId = masterId;
		this.localId  = localId;
		this.creationTime = new Date();
	}
	
	private Date creationTime;
	private int masterId;
	private int localId;
	
	public Date getCreationTime() {
		return creationTime;
	}
	public LinkItem setCreationTime(Date creationTime) {
		this.creationTime = creationTime;
		return this;
	}
	public int getMasterId() {
		return masterId;
	}
	public LinkItem setMasterId(int masterId) {
		this.masterId = masterId;
		return this;
	}
	public int getLocalId() {
		return localId;
	}
	public LinkItem setLocalId(int localId) {
		this.localId = localId;
		return this;
	}
	
}
