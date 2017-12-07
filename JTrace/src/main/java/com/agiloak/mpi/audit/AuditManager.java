package com.agiloak.mpi.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.persistence.AuditDAO;

/*
 * Simple manager to enforce business rules before saving the entity. 
 * This is exposed to the UI and UKRDCIndexManager instead of the AuditDAO.
 * Some functions (find and delete) are only available in the DAO as they are currently only used for testing and will never be exposed
 * via an API 
 */

public class AuditManager {
	
	private final static Logger logger = LoggerFactory.getLogger(AuditManager.class);
	
	public Audit create(int type, int personId, int masterId, String desc) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		if ( masterId==0 ) {
			throw new MpiException("Master Id must be provided");
		}
		if ( (desc==null) || (desc.length()==0) ) {
			throw new MpiException("Description must be provided");
		}
		logger.debug("New Work Item");
		
		Audit audit = new Audit(type, personId, masterId, desc);
		AuditDAO.create(audit);
			
		return audit;
		
	}

}
