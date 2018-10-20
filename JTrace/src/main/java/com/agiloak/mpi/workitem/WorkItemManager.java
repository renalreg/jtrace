package com.agiloak.mpi.workitem;

import java.util.Date;
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
	
	/**
	 * Create a new Work Item 
	 *
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 * @return The WorkItem following creation.
	 * @throws MpiException For any exception encountered. 
	 */
	public WorkItem create(int type, int personId, int masterId, String desc) throws MpiException {
		
		return create(type, personId, masterId, desc, new HashMap<String,String>());

	}

	/**
	 * Create a new Work Item 
	 *
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 * @param attributes The additional attributes which vary by event
	 * @return The WorkItem following creation.
	 * @throws MpiException For any exception encountered. 
	 */
	public WorkItem create(int type, int personId, int masterId, String desc, Map<String,String> attributes) throws MpiException {

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
			workItem = new WorkItem(type, personId, masterId, desc, attributes);
			WorkItemDAO.create(workItem);
			
			// 2 - AUDIT
			Map<String,String> attr = new HashMap<String, String>();
			attr.put("Id", Integer.toString(workItem.getId()));
			attr.put("Type", Integer.toString(workItem.getType()));
			AuditManager am = new AuditManager();
			//am.create(Audit.WORK_ITEM_CREATED, personId, masterId, "WI CREATED", attr);
		}
			
		return workItem;
		
	}

	/**
	 * Update the Work Item using the id as the key. Certain values are not updateable as they are intrinsic
	 * to the WorkItem (personId, masterId, type). Last updated date will automatically be updated
	 *
	 * @param workItemId - REQUIRED - The id of the WorkItem being updated. This must exist
	 * @param status - REQUIRED - The new status of the WorkItem {@link WorkItemStatus}
	 * @param updateDesc - REQUIRED - Description of the work item update
	 * @param updatedBy - REQUIRED - Who is updating the item
	 * @return The WorkItem following the update
	 * @throws MpiException For any exception encountered. 
	 */
	public WorkItem update(int workItemId, int status, String updateDesc, String updatedBy) throws MpiException {

		logger.debug("Updating Work Item");

		if ( workItemId==0 ) {
			throw new MpiException("WorkItem Id must be provided");
		}
		if ( status == 0 ) {
			throw new MpiException("Status must be provided");
		}
		if ( status > 3 ) {
			throw new MpiException("Status provided is not valid");
		}
		if ( (updateDesc==null) || (updateDesc.length()==0) ) {
			throw new MpiException("Description must be provided");
		}
		if ( updatedBy==null || (updatedBy.length()==0)) {
			throw new MpiException("Updated By user must be provided");
		}
		
		WorkItem workItem = WorkItemDAO.get(workItemId);
		
		if (workItem == null) {
			logger.error("Cannot update - work item does not exist");
			throw new MpiException("Cannot update - work item does not exist");
		} else {
			//only update the allowable fields
			workItem.setLastUpdated(new Date());
			workItem.setStatus(status);
			workItem.setUpdatedBy(updatedBy);
			workItem.setUpdateDesc(updateDesc);
			
			WorkItemDAO.update(workItem);
			
			// AUDIT
			Map<String,String> attr = new HashMap<String, String>();
			attr.put("Id", Integer.toString(workItem.getId()));
			attr.put("Status", Integer.toString(workItem.getStatus()));
			attr.put("UpdatedBy", workItem.getUpdatedBy());
			attr.put("UpdateDesc", workItem.getUpdateDesc());
			AuditManager am = new AuditManager();
			am.create(Audit.WORK_ITEM_CREATED, workItem.getPersonId(), workItem.getMasterId(), "WI UPDATED", attr);
		}
		
		return workItem;
	}

	/**
	 * @param personId - Identifies the person record for which the work items are required
	 * @return List of WorkItems for the person
	 * @throws MpiException For any exception encountered. 
	 */
	public List<WorkItem> findByPerson(int personId) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		logger.debug("Find work items for personId:"+personId);

		return WorkItemDAO.findByPerson(personId);
		
	}

	/**
	 * @param personId - Identifies the person record for which the work items are to be deleted
	 * @throws MpiException For any exception encountered. 
	 */
	public void deleteByPerson(int personId) throws MpiException {

		if ( personId==0 ) {
			throw new MpiException("Person Id must be provided");
		}
		logger.debug("Delete work items for personId:"+personId);

		WorkItemDAO.deleteByPerson(personId);
		
	}
	
	/**
	 * @param masterId - Identifies the master record for which the work items are required
	 * @throws MpiException For any exception encountered. 
	 */
	public void deleteByMaster(int masterId) throws MpiException {

		if ( masterId==0 ) {
			throw new MpiException("Master Id must be provided");
		}
		logger.debug("Delete work items for masterId:"+masterId);

		WorkItemDAO.deleteByMasterId(masterId);
		
	}
	
}
