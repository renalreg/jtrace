package com.agiloak.mpi.trace;

import java.util.ArrayList;
import java.util.List;

public class TraceResponse {
	
	private String traceId; 
	private String traceStartTime; 
	private String traceEndTime; 

	private String message; 
	private String status;
	private double maxWeight;
	private int matchCount;
	
	private List<TraceResponseLine> responseLines;

	public TraceResponse(){
		this.maxWeight = 0.00;
		this.matchCount = 0;
		this.responseLines = new ArrayList<TraceResponseLine>();
		
	}
	
	public String getTraceId() {
		return traceId;
	}

	public void setTraceId(String traceId) {
		this.traceId = traceId;
	}

	public String getTraceStartTime() {
		return traceStartTime;
	}

	public void setTraceStartTime(String traceStartTime) {
		this.traceStartTime = traceStartTime;
	}

	public String getTraceEndTime() {
		return traceEndTime;
	}

	public void setTraceEndTime(String traceEndTime) {
		this.traceEndTime = traceEndTime;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public double getMaxWeight() {
		return maxWeight;
	}

	public void setMaxWeight(double maxWeight) {
		this.maxWeight = maxWeight;
	}

	public int getMatchCount() {
		return matchCount;
	}

	public void setMatchCount(int matchCount) {
		this.matchCount = matchCount;
	}

	public List<TraceResponseLine> getResponseLines() {
		return responseLines;
	}

	public void addResponseLines(TraceResponseLine responseLine) {
		this.responseLines.add(responseLine);
	}

}
