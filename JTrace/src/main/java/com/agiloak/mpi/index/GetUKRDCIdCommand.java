package com.agiloak.mpi.index;

import java.sql.Connection;

import com.agiloak.mpi.index.persistence.MasterRecordDAO;

public class GetUKRDCIdCommand extends APICommand {
	
	private int masterId;

	public GetUKRDCIdCommand(int masterId) {
		this.masterId = masterId;
	}

	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		MasterRecord master = MasterRecordDAO.get(conn, masterId);
		if (master == null) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage("Master ID does not exist");
		} else {
			if (master.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE)) {
				resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
				resp.setNationalIdentity(new NationalIdentity(master.getNationalIdType(), master.getNationalId()));
			} else {
				resp.setStatus(UKRDCIndexManagerResponse.FAIL);
				resp.setMessage("Master ID is not a UKRDC ID");
			}
		}
		return resp;

	}
	
}
