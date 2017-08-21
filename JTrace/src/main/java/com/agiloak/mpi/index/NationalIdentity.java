package com.agiloak.mpi.index;

public class NationalIdentity {

	public NationalIdentity() {
	}
	public NationalIdentity(String type, String id) {
		this.type = type;
		this.id = id;
	}
	
	private String type;
	private String id;
	public String getType() {
		return type;
	}
	public NationalIdentity setType(String type) {
		this.type = type;
		return this;
	}
	public String getId() {
		return id;
	}
	public NationalIdentity setId(String id) {
		this.id = id;
		return this;
	}
	
}
