package com.agiloak.mpi.index;

import java.sql.Timestamp;

public class PidXREF {
	
	public PidXREF(String sendingFacility, String sendingExtract, String localId) {
		this.sendingFacility = sendingFacility;
		this.sendingExtract  = sendingExtract;
		this.localId = localId;
	}
	public PidXREF() {
	}
	
	private int id;
	
	private String pid;
	private Timestamp lastUpdated;
	private Timestamp creationDate;
	private String sendingFacility;
	private String sendingExtract;
	private String localId;
	
	public int getId() {
		return id;
	}

	public PidXREF setId(int id) {
		this.id = id;
		return this;
	}

	public String getPid() {
		return pid;
	}

	public PidXREF setPid(String pid) {
		this.pid = pid;
		return this;
	}

	public Timestamp getLastUpdated() {
		return lastUpdated;
	}

	public PidXREF setLastUpdated(Timestamp lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}

	public Timestamp getCreationDate() {
		return creationDate;
	}

	public PidXREF setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
		return this;
	}

	public String getSendingFacility() {
		return sendingFacility;
	}
	
	public PidXREF setSendingFacility(String sendingFacility) {
		this.sendingFacility = sendingFacility;
		return this;
	}

	public String getSendingExtract() {
		return sendingExtract;
	}
	
	public PidXREF setSendingExtract(String sendingExtract) {
		this.sendingExtract = sendingExtract;
		return this;
	}
	
	public String getLocalId() {
		return localId;
	}

	public PidXREF setLocalId(String localId) {
		this.localId = localId;
		return this;
	}

}
