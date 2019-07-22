package com.agiloak.mpi.index;

import java.sql.Connection;

public class ValidateCommand extends APICommand {
	
	private Person person;

	public ValidateCommand(Person person) {
		this.person = person;
	}

	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		im.validateInternal(person);
		im.validateWithEMPI(conn, person);
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		return resp;

	}
	
}
