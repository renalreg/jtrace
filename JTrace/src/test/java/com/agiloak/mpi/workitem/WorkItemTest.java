package com.agiloak.mpi.workitem;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class WorkItemTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		// delete test data
		WorkItemDAO.deleteByPerson(1);
		WorkItemDAO.deleteByPerson(2);
	}

	@Test
	public void testCreate() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(WorkItem.TYPE_INVESTIGATE_DUE_TO_CHANGED_DEMOG, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(1);
		assert(workItems.size()==1);
		
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);
	}

	@Test
	public void testDelete() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(WorkItem.TYPE_INVESTIGATE_DUE_TO_CHANGED_DEMOG, 2, "test");
		wim.create(WorkItem.TYPE_INVESTIGATE_DUE_TO_CHANGED_DEMOG, 2, "test2");
		assert(true);
		List<WorkItem> workItems = wim.findByPerson(2);
		assert(workItems.size()==2);
		
		wim.deleteByPerson(2);
		List<WorkItem> workItems2 = wim.findByPerson(2);
		assert(workItems2.size()==0);
		
	}

	@Test
	public void testDeleteNoPerson() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.deleteByPerson(0);
		assert(true);
	}

	@Test
	public void testCreateNoPerson() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_DEMOGS_NEAR_MATCH,0,"test");
	}

	@Test
	public void testCreateNullDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_DEMOGS_NEAR_MATCH,1,null);
	}

	@Test
	public void testCreateEmptyDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID,1,"");
	}
}
