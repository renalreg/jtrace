package com.agiloak.mpi.workitem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class WorkItemManager {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemManager.class);
	
	public void create(String desc) throws MpiException {

		if ( (desc==null) || (desc.length()==0) ) {
			throw new MpiException("Description must be provided");
		}
		logger.debug("New Work Item:"+desc);
		

		WorkItem wi = new WorkItem(desc);
		
		WorkItemDAO.insert(wi);
		
		
	}
	
}
