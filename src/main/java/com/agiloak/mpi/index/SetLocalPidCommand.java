package com.agiloak.mpi.index;

import java.sql.Connection;

public class SetLocalPidCommand extends APICommand {
	
	private Person person;
	private String sendingFacility;
	private String sendingExtract;

	public SetLocalPidCommand(Person person, String sendingFacility, String sendingExtract) {
		this.person = person;
		this.sendingFacility = sendingFacility;
		this.sendingExtract = sendingExtract;
	}
	
	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		person.getResolvedValidationWorkItemIds().clear();

		im.standardise(person);

		String pid = im.setLocalPIDInternal(conn, person, sendingFacility, sendingExtract);
		
		if (pid==null) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage("FAILED TO MATCH DURING UPDATE - SEE WORK ITEMS");
		} else {
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
			im.stageSourceWorkItemClosures(conn, person, sendingFacility, sendingExtract);
			resp.setPid(pid);
		}
		
		return resp;
	}
	
}
