package com.agiloak.mpi.workitem;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
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
	
	public static int testWorkItemId = 0;
	
	public static Connection conn = null;
	@Before
	public void openConnection()  throws MpiException {
		conn = SimpleConnectionManager.getDBConnection();
	}
	@After
	public void closeConnection()  throws MpiException {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new MpiException("FAILED TO CLOSE CONNECTION");
		}
	}

	@BeforeClass
	public static void setupWrapper()  throws MpiException, SQLException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();
		setup();
		conn.close();
	}

	public static void setup()  throws MpiException {
		WorkItemDAO.deleteByPerson(conn, 200001);
		WorkItemDAO.deleteByPerson(conn, 200002);
		WorkItemDAO.deleteByPerson(conn, 200003);
		WorkItemDAO.deleteByPerson(conn, 200004);
		WorkItemDAO.deleteByPerson(conn, 200005);
		WorkItemDAO.deleteByPerson(conn, 200006);
		
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200005, 1, "test");
		
		testWorkItemId = wi1.getId();
	}

	@Test
	public void testCreate() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200001, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(conn, 200001);
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
		WorkItem wi1 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200002, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(conn, 200002);
		assert(workItems.size()==1);
		
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);
		
		WorkItemManagerResponse resp = wim.update(Integer.toString(wi2.getId()), Integer.toString(WorkItemStatus.STATUS_CLOSED), "Not a match - do not try to resolve", "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.SUCCESS);
		assert(resp.getWorkItem()!=null);
		assert(resp.getWorkItem().getId()==wi2.getId());
		
		workItems = wim.findByPerson(conn, 200002);
		assert(workItems.size()==1);
		
		wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));

		assert(wi2.getStatus()==WorkItemStatus.STATUS_CLOSED);
		assert(wi2.getUpdateDesc().equals("Not a match - do not try to resolve"));
		assert(wi2.getUpdatedBy().equals("NJONES02"));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated()) > 0);
		
	}

	@Test
	public void testUpdateNoId() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(0, WorkItemStatus.STATUS_CLOSED, "Not a match - do not try to resolve", "NJONES02" );
		WorkItemManagerResponse resp = wim.update("0", Integer.toString(WorkItemStatus.STATUS_CLOSED), "Not a match - do not try to resolve", "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}

	@Test
	public void testUpdateBadStatus1() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(testWorkItemId, 0, "Not a match - do not try to resolve", "NJONES02" );
		WorkItemManagerResponse resp = wim.update(Integer.toString(testWorkItemId), "0", "Not a match - do not try to resolve", "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}

	@Test
	public void testUpdateBadStatus2() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(testWorkItemId, 99, "Not a match - do not try to resolve", "NJONES02" );
		WorkItemManagerResponse resp = wim.update(Integer.toString(testWorkItemId), "99", "Not a match - do not try to resolve", "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}

	@Test
	public void testUpdateBadStatus3() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(testWorkItemId, 99, "Not a match - do not try to resolve", "NJONES02" );
		WorkItemManagerResponse resp = wim.update(Integer.toString(testWorkItemId), "B", "Not a match - do not try to resolve", "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}

	@Test
	public void testExplicitUpdateNoUser() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(testWorkItemId, WorkItemStatus.STATUS_CLOSED, "Not a match - do not try to resolve", null );
		WorkItemManagerResponse resp = wim.update(Integer.toString(testWorkItemId), Integer.toString(WorkItemStatus.STATUS_CLOSED), "Not a match - do not try to resolve", null );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}
	
	@Test
	public void testExplicitUpdateNoUpdateDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		//wim.update(testWorkItemId, WorkItemStatus.STATUS_CLOSED,  null, "NJONES02" );
		WorkItemManagerResponse resp = wim.update(Integer.toString(testWorkItemId), Integer.toString(WorkItemStatus.STATUS_CLOSED), null, "NJONES02" );
		assert(resp.getStatus()==WorkItemManagerResponse.FAIL);
	}

	@Test
	public void testUpdateIfExists() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		WorkItem wi1 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200003, 1, "test");
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(conn, 200003);
		assert(workItems.size()==1);
	
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);

		WorkItem wi3 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200003, 1, "test2");
		assert(wi3.getId()==wi1.getId());

	}
	@Test
	public void testDelete() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 2, "test");
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 3, "test2");
		assert(true);
		List<WorkItem> workItems = wim.findByPerson(conn, 200004);
		assert(workItems.size()==2);
		
		wim.deleteByPerson(conn, 200004);
		List<WorkItem> workItems2 = wim.findByPerson(conn, 200004);
		assert(workItems2.size()==0);
		
	}

	@Test
	public void testDeleteNoPerson() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.deleteByPerson(conn, 0);
		assert(true);
	}

	@Test
	public void testDeleteByMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 2, "test");
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200004, 3, "test2");
		assert(true);
		List<WorkItem> workItems = wim.findByPerson(conn, 200004);
		assert(workItems.size()==2);
		
		wim.deleteByMaster(conn, 3);
		List<WorkItem> workItems2 = wim.findByPerson(conn, 200004);
		assert(workItems2.size()==1);
		
	}
	@Test
	public void testDeleteyMasterNoMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.deleteByMaster(conn, 0);
		assert(true);
	}
	@Test
	public void testCreateNoPerson() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,0,1,"test");
	}

	@Test
	public void testCreateNoMaster() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,0,0,"test");
	}
	@Test
	public void testCreateNullDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,1,2,null);
	}

	@Test
	public void testCreateEmptyDesc() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);
		wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,1,3,"");
	}
	@Test
	public void testCreateWithAttributes() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		WorkItem wi1 = wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 200006, 1, "test", attr);
		assert(true);

		List<WorkItem> workItems = wim.findByPerson(conn, 200006);
		assert(workItems.size()==1);
		
		WorkItem wi2 = workItems.get(0);
		assert(wi2.getId()==wi1.getId());
		assert(wi2.getPersonId()==wi1.getPersonId());
		assert(wi2.getStatus()==wi1.getStatus());
		assert(wi2.getType()==wi1.getType());
		assert(wi2.getDescription().equals(wi1.getDescription()));
		assert(wi2.getLastUpdated().compareTo(wi1.getLastUpdated())==0);
		assert(wi2.getAttributes().size()==2);
		assert(wi2.getAttributes().get("TestK1").equals("TestV1"));
		assert(wi2.getAttributes().get("TestK2").equals("TestV2"));
		
	}
	
	
}
