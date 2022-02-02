package com.agiloak.mpi.workitem;

public class WorkItemManagerResponse {

	public final static int SUCCESS = 0;
	public final static int FAIL = 1;
	
	protected int status = SUCCESS;
	protected String message = "";
	protected String stackTrace = "";
	protected WorkItem workItem;
	
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
	public WorkItem getWorkItem() {
		return workItem;
	}
	public void setWorkItem(WorkItem workItem) {
		this.workItem = workItem;
	}

}
