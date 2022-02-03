package com.agiloak.mpi.index;

import java.sql.Connection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditManager;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;

public class UnlinkCommand extends APICommand {
	
	private final static Logger logger = LoggerFactory.getLogger(UnlinkCommand.class);

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
		
		// Validate parms
		if (personId==0 || masterId==0 || reason==null || reason.length()==0 || user==null || user.length()==0 ) { 
			logger.error("Incomplete parameters for unlink");
			throw new MpiException("Incomplete parameters for unlink");
		}

		// Validate entities
		MasterRecord master = MasterRecordDAO.get(conn, masterId);
		if (master==null) {
			logger.error("Master Record does not exist");
			throw new MpiException("Master Record does not exists");
		}
		Person person = PersonDAO.get(conn, personId);
		if (person==null) {
			logger.error("Person does not exist");
			throw new MpiException("Person does not exists");
		}
		
		// unlink
		im.unlinkInternal(conn, personId, masterId, user, reason);
		
		// correct the master record
		im.resetMaster(conn, master, user, reason);
		
		// if this is a UKRDC reset then create a new UKRDC for this person
		if (master.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE)) {
			person.setEffectiveDate(new Date());
			im.createUKRDCLink(conn, person);
		}
		
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		return resp;
	}
	
}
