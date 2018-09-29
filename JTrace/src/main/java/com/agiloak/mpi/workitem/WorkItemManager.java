package com.agiloak.mpi.workitem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditManager;
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
		logger.debug("New Work Item");
		
		WorkItem workItem = WorkItemDAO.findByPersonAndMaster(personId, masterId);
		if (workItem == null) {
			workItem = new WorkItem(type, personId, masterId, desc);
			WorkItemDAO.create(workItem);
			
			// 2 - AUDIT
			Map<String,String> attr = new HashMap<String, String>();
			attr.put("Id", Integer.toString(workItem.getId()));
			attr.put("Type", Integer.toString(workItem.getType()));
			AuditManager am = new AuditManager();
			am.create(Audit.WORK_ITEM_CREATED, personId, masterId, "WI CREATED", attr);
		}
			
		return workItem;
		
	}

	public WorkItem update(WorkItem item) throws MpiException {

		logger.debug("Updating Work Item");

		if ( item.getPersonId()==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		if ( item.getMasterId()==0 ) {
			throw new MpiException("Master Id must be provided");
		}
		if ( (item.getDescription()==null) || (item.getDescription().length()==0) ) {
			throw new MpiException("Description must be provided");
		}
		if ( item.getUpdatedBy()==null || (item.getUpdatedBy().length()==0)) {
			throw new MpiException("Updated By user must be provided");
		}
		if ( item.getUpdateDesc()==null || (item.getUpdateDesc().length()==0)) {
			throw new MpiException("Update Description must be provided");
		}
		
		WorkItem workItem = WorkItemDAO.findByPersonAndMaster(item.getPersonId(), item.getMasterId());
		if (workItem == null) {
			logger.error("Cannot update - work item does not exist");
			throw new MpiException("Cannot update - work item does not exist");
		} else {
			//only update the allowable fields
			workItem.setLastUpdated(item.getLastUpdated());
			workItem.setStatus(item.getStatus());
			workItem.setUpdatedBy(item.getUpdatedBy());
			workItem.setUpdateDesc(item.getUpdateDesc());
			
			WorkItemDAO.update(workItem);
			
			// AUDIT
			Map<String,String> attr = new HashMap<String, String>();
			attr.put("Id", Integer.toString(workItem.getId()));
			attr.put("Type", Integer.toString(workItem.getType()));
			attr.put("Status", Integer.toString(workItem.getStatus()));
			attr.put("UpdatedBy", workItem.getUpdatedBy());
			attr.put("UpdateDesc", workItem.getUpdateDesc());
			AuditManager am = new AuditManager();
			am.create(Audit.WORK_ITEM_CREATED, item.getPersonId(), item.getMasterId(), "WI UPDATED", attr);
		}
		
		return item;
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
	
	public void deleteByMaster(int masterId) throws MpiException {

		if ( masterId==0 ) {
			throw new MpiException("Master Id must be provided");
		}
		logger.debug("Delete work items for masterId:"+masterId);

		WorkItemDAO.deleteByMasterId(masterId);
		
	}
	
}
