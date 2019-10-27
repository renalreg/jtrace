package com.agiloak.mpi.audit;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.NationalIdentity;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;

/*
 * Simple manager to enforce business rules before saving the entity. 
 * This is exposed to the UI and UKRDCIndexManager instead of the AuditDAO.
 * Some functions (find and delete) are only available in the DAO as they are currently only used for testing and will never be exposed
 * via an API 
 */

public class AuditManager {
	
	private final static Logger logger = LoggerFactory.getLogger(AuditManager.class);
	
	public Audit create(Connection conn, int type, int personId, int masterId, String desc, Map<String,String> attributes) throws MpiException {

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
		
		NationalIdentity mainNationalId = getMainNationalIdentity(conn, personId);
		
		Audit audit = new Audit(type, personId, masterId, desc, mainNationalId, attributes);
		AuditDAO.create(conn, audit);
			
		return audit;
		
	}
	
	// Convenience method when no attributes
	public Audit create(Connection conn, int type, int personId, int masterId, String desc) throws MpiException {

		return create(conn, type, personId, masterId, desc, null);		
	}
	
	private NationalIdentity getMainNationalIdentity(Connection conn, int personId) throws MpiException {
		
		NationalIdentity mainNationalIdentity = getMainNationalIdentity(conn, personId, NationalIdentity.NHS_TYPE);
		if (mainNationalIdentity==null) mainNationalIdentity = getMainNationalIdentity(conn, personId, NationalIdentity.CHI_TYPE);
		if (mainNationalIdentity==null) mainNationalIdentity = getMainNationalIdentity(conn, personId, NationalIdentity.HSC_TYPE);
		
		// If none of the main ids are present, just get the first NI on the person
		if (mainNationalIdentity==null) mainNationalIdentity = getFirstNationalIdentity(conn, personId);
		
		return mainNationalIdentity;
	}
	
	private NationalIdentity getMainNationalIdentity(Connection conn, int personId, String idType) throws MpiException {
		
		NationalIdentity mainNationalIdentity = null;
		LinkRecord link = LinkRecordDAO.findByPersonAndType(conn, personId, idType);
		if (link != null) {
			MasterRecord master = MasterRecordDAO.get(conn, link.getMasterId());
			mainNationalIdentity = new NationalIdentity(idType, master.getNationalId());
		}
		
		return mainNationalIdentity;
	}
	private NationalIdentity getFirstNationalIdentity(Connection conn, int personId) throws MpiException {
		
		NationalIdentity mainNationalIdentity = null;
		List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, personId);
		if (links.size() > 0) {
			MasterRecord master = MasterRecordDAO.get(conn, links.get(0).getMasterId());
			mainNationalIdentity = new NationalIdentity(master.getNationalIdType(), master.getNationalId());
		}
		
		return mainNationalIdentity;
	}

}
