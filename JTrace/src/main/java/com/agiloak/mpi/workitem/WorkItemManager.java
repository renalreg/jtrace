package com.agiloak.mpi.workitem;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

/*
 * Simple manager to enforce business rules before saving the entity. 
 * This is exposed to the UI and UKRDCIndexManager instead of the WorkItemDAO. 
 */

public class WorkItemManager {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemManager.class);
	
	public WorkItem create(int type, int personId, int masterId, String desc) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		if ( masterId==0 ) {
			throw new MpiException("Master Id must be provided");
		}
		if ( (desc==null) || (desc.length()==0) ) {
			throw new MpiException("Description must be provided");
		}
		logger.debug("New Work Item:"+desc);
		
		WorkItem workItem = WorkItemDAO.findByPersonAndMaster(personId, masterId);
		if (workItem == null) {
			workItem = new WorkItem(type, personId, masterId, desc);
			WorkItemDAO.create(workItem);
		}
			
		return workItem;
		
	}


	public List<WorkItem> findByPerson(int personId) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		logger.debug("Find work items for personId:"+personId);

		return WorkItemDAO.findByPerson(personId);
		
	}

	public void deleteByPerson(int personId) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		logger.debug("Delete work items for personId:"+personId);

		WorkItemDAO.deleteByPerson(personId);
		
	}
	
}
