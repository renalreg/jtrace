package com.agiloak.mpi.index;

public class PidXREF {
	
	public PidXREF(String sendingFacility, String sendingExtract, String localPatientId) {
		this.sendingFacility = sendingFacility;
		this.sendingExtract  = sendingExtract;
		this.localPatientId = localPatientId;
	}
	
	private int id;
	
	private String pid;

	private String sendingFacility;
	private String sendingExtract;
	private String localPatientId;
	
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
	
	public String getLocalPatientId() {
		return localPatientId;
	}

	public PidXREF setLocalPatientId(String localPatientId) {
		this.localPatientId = localPatientId;
		return this;
	}

}
