package com.agiloak.mpi.index;

import java.sql.Connection;

public class UnlinkCommand extends APICommand {
	
	private int personId;
	private int masterId;
	private String user;
	private String reason;
	
	public UnlinkCommand(int personId, int masterId, String user, String reason) {
		this.personId = personId;
		this.masterId = masterId;
		this.user = user;
		this.reason = reason;
	}
	
	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		
		// unlink
		im.unlinkInternal(conn, personId, masterId, user, reason);
		
		// correct the master record
		im.resetMaster(conn, masterId, user, reason);
		
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		return resp;
	}
	
}
