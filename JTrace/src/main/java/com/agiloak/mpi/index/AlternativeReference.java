package com.agiloak.mpi.index;

public class AlternativeReference {

	private long arid;
	public long getArid() {
		return arid;
	}
	public void setArid(long arid) {
		this.arid = arid;
	}
	private String id;
	private String type;
	private String originator;
	private String supercededBy;
	private long masterId;
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getOriginator() {
		return originator;
	}
	public void setOriginator(String originator) {
		this.originator = originator;
	}
	public String getSupercededBy() {
		return supercededBy;
	}
	public void setSupercededBy(String supercededBy) {
		this.supercededBy = supercededBy;
	}
	public long getMasterId() {
		return masterId;
	}
	public void setMasterId(long masterId) {
		this.masterId = masterId;
	}
}
