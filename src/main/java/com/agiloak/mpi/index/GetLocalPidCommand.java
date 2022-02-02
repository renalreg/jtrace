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

		im.standardise(person);
		String outcome = im.getLocalPIDInternal(conn, person, sendingFacility, sendingExtract);
		if (outcome.equals("REJECT")) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage("FAILED TO MATCH DURING VALIDATION - SEE WORK ITEMS");
		} else {
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
			resp.setPid(outcome); 
		}
		
		return resp;
	}
	
}
