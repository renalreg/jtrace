package com.agiloak.mpi.workitem;

import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class WorkItemTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		// delete test data
		WorkItemDAO.deleteByPerson(200001);
		WorkItemDAO.deleteByPerson(200002);
		WorkItemDAO.deleteByPerson(200003);
		WorkItemDAO.deleteByPerson(200004);
	}
	
	@Test
	public void testCreate() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200001, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(200001);
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
	public void testExplicitUpdate() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200002, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(200002);
		assert(workItems.size()==1);
		
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);
		
		wi2.setStatus(WorkItem.STATUS_CLOSED);
		wi2.setUpdatedBy("NJONES02");
		wi2.setUpdateDesc("Not a match - do not try to resolve");
		wi2.setLastUpdated(new Date());
		wim.update(wi2);
	}
	@Test
	public void testExplicitUpdateNoUser() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi = new WorkItem(1,1,1,"DESC");
		wi.setStatus(WorkItem.STATUS_CLOSED);
		//wi.setUpdatedBy("NJONES02");
		wi.setUpdateDesc("Not a match - do not try to resolve");
		wi.setLastUpdated(new Date());
		exception.expect(MpiException.class);
		wim.update(wi);
	}
	@Test
	public void testExplicitUpdateNoUpdateDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi = new WorkItem(1,1,1,"DESC");
		wi.setStatus(WorkItem.STATUS_CLOSED);
		wi.setUpdatedBy("NJONES02");
		//wi.setUpdateDesc("Not a match - do not try to resolve");
		wi.setLastUpdated(new Date());
		exception.expect(MpiException.class);
		wim.update(wi);
	}

	@Test
	public void testUpdateIfExists() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200003, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(200003);
		assert(workItems.size()==1);
	
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);

		WorkItem wi3 = wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200003, 1, "test2");
		assert(wi3.getId()==wi1.getId());

	}
	@Test
	public void testDelete() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 2, "test");
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 3, "test2");
		assert(true);
		List<WorkItem> workItems = wim.findByPerson(200004);
		assert(workItems.size()==2);
		
		wim.deleteByPerson(200004);
		List<WorkItem> workItems2 = wim.findByPerson(200004);
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
	public void testDeleteByMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 2, "test");
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 3, "test2");
		assert(true);
		List<WorkItem> workItems = wim.findByPerson(200004);
		assert(workItems.size()==2);
		
		wim.deleteByMaster(3);
		List<WorkItem> workItems2 = wim.findByPerson(200004);
		assert(workItems2.size()==1);
		
	}
	@Test
	public void testDeleteyMasterNoMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.deleteByMaster(0);
		assert(true);
	}
	@Test
	public void testCreateNoPerson() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,0,1,"test");
	}

	@Test
	public void testCreateNoMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,0,0,"test");
	}
	@Test
	public void testCreateNullDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,1,2,null);
	}

	@Test
	public void testCreateEmptyDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,1,3,"");
	}
}
