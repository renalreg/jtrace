package com.agiloak.mpi.index;

import java.sql.Connection;
import java.util.List;

import com.agiloak.mpi.workitem.WorkItemManager;

public class StoreCommand extends APICommand {
	
	private Person person;

	public StoreCommand(Person person) {
		this.person = person;
	}

	public UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception {
		
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		List<Integer> resolvedItems = im.findResolvedWorkItems(conn, person);
		NationalIdentity natId = im.createOrUpdate(conn, person);
		resolvedItems.addAll(person.getResolvedValidationWorkItemIds());
		WorkItemManager workItems = new WorkItemManager();
		for (Integer id : resolvedItems) {
			workItems.closeAutomatically(conn, id);
		}
		resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		resp.setNationalIdentity(natId);
		return resp;

	}
	
}
