package com.agiloak.mpi.index;

import java.sql.Connection;

public class StoreCommand extends APICommand {
	
	private Person person;

	public StoreCommand(Person person) {
		this.person = person;
	}

	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		NationalIdentity natId = im.createOrUpdate(conn, person);
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		resp.setNationalIdentity(natId);
		return resp;

	}
	
}
