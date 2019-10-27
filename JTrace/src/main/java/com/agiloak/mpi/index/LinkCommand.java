package com.agiloak.mpi.index;

import java.sql.Connection;

public class LinkCommand extends APICommand {
	
	private int personId;
	private int masterId;
	private String user;
	private int linkCode;
	private String linkDesc;
	
	public LinkCommand(int personId, int masterId, String user, int linkCode, String linkDesc) {
		this.personId = personId;
		this.masterId = masterId;
		this.user = user;
		this.linkCode = linkCode;
		this.linkDesc = linkDesc;
	}
	
	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		im.linkInternal(conn, personId, masterId, user, linkCode, linkDesc);
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		return resp;
	}
	
}
