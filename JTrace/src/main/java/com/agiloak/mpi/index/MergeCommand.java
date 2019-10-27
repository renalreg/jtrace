package com.agiloak.mpi.index;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;

public class MergeCommand extends APICommand {
	
	private int superceedingId;
	private int supercededId;

	public MergeCommand(int superceedingId, int supercededId) {
		this.superceedingId = superceedingId;
		this.supercededId = supercededId;
	}

	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		// 0 - Get Details
		MasterRecord superceeding = MasterRecordDAO.get(conn, superceedingId);
		MasterRecord superceded   = MasterRecordDAO.get(conn, supercededId);
		// 1 - LINK
		// For every record linking to the superceeded record id
		List<LinkRecord> links = LinkRecordDAO.findByMaster(conn, supercededId);
		for (LinkRecord link : links) {
			LinkRecordDAO.delete(conn, link);

			LinkRecord newLink = new LinkRecord(superceedingId, link.getPersonId());
			LinkRecordDAO.create(conn, newLink);

			// 2 - AUDIT
			Map<String,String> attr = new HashMap<String, String>();
			attr.put("SuperceedingMaster", Integer.toString(superceedingId));
			attr.put("SupercededMaster", Integer.toString(supercededId));
			attr.put("SuperceedingUKRDC", superceeding.getNationalId());
			attr.put("SupercededUKRDC", superceded.getNationalId());
			AuditManager am = new AuditManager();
			am.create(conn, Audit.UKRDC_MERGE, link.getPersonId(), superceedingId, "UKRDC Merge", attr);
			
		}
		// 3 - MASTER RECORD
		MasterRecordDAO.delete(conn, supercededId);
		return resp;

	}
	
}
