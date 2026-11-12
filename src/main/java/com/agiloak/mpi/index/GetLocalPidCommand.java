package com.agiloak.mpi.index;

import java.sql.Connection;

public class GetLocalPidCommand extends APICommand {
	
	private Person person;
	private String sendingFacility;
	private String sendingExtract;

	public GetLocalPidCommand(Person person, String sendingFacility, String sendingExtract) {
		this.person = person;
		this.sendingFacility = sendingFacility;
		this.sendingExtract = sendingExtract;
	}
	
	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		person.getResolvedValidationWorkItemIds().clear();

		im.standardise(person);
		String outcome = im.getLocalPIDInternal(conn, person, sendingFacility, sendingExtract);
		if (outcome != null && outcome.startsWith("REJECT")) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			String workItemType = outcome.length() > "REJECT:".length() ? outcome.substring("REJECT:".length()) : "unknown";
			resp.setMessage("FAILED TO MATCH DURING VALIDATION - SEE WORK ITEM TYPE: "+workItemType);
		} else if (outcome == null) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage("GET LOCAL PID RETURNED NO OUTCOME");
		} else {
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
			im.stageSourceWorkItemClosures(conn, person, sendingFacility, sendingExtract);
			resp.setPid(outcome);
		}
		
		return resp;
	}
	
}
