package com.agiloak.mpi.index;

public class UKRDCIndexManagerResponse {

	public final static int SUCCESS = 0;
	public final static int FAIL = 1;
	
	protected int status = SUCCESS;
	protected String message = "";
	protected String stackTrace = "";
	protected NationalIdentity nationalIdentity;
	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getStackTrace() {
		return stackTrace;
	}
	public void setStackTrace(String stackTrace) {
		this.stackTrace = stackTrace;
	}
	public NationalIdentity getNationalIdentity() {
		return nationalIdentity;
	}
	public void setNationalIdentity(NationalIdentity nationalIdentity) {
		this.nationalIdentity = nationalIdentity;
	}

}
