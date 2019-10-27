package com.agiloak.mpi.index;

import java.sql.Connection;

public class SearchCommand extends APICommand {
	
	private ProgrammeSearchRequest psr;
	
	public SearchCommand(ProgrammeSearchRequest psr) {
		this.psr = psr;
	}
	
	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		String ukrdcId = im.searchInternal(conn, psr);
		NationalIdentity natId = new NationalIdentity(ukrdcId);
		resp.setNationalIdentity(natId);
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		return resp;

	}
	
}
