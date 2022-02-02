package com.agiloak.mpi.workitem;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;

public class WorkItemAttributesTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		//SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		//AuditDAO.deleteByPerson(1);
	}

	@Test
	public void testSimpleJsonify() throws MpiException {
		
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		WorkItem workItem = new WorkItem(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 1, 1, "test", attr);
		assert(workItem.getAttributesJson().length()>0);
	}
	@Test
	public void testHydration() throws MpiException {
		
		//SETUP
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		WorkItem workItem = new WorkItem(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 1, 1, "test", attr);
		assert(workItem.getAttributesJson().length()>0);

		//TEST
		WorkItem workItem2 = new WorkItem(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 1, 1, "test", workItem.getAttributesJson());
		assert(workItem2.getAttributes().size()==2);
	}
	
	@Test
	public void testNoAttributes() throws MpiException {
		
		WorkItem workItem = new WorkItem(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 1, 1, "test", new HashMap<String, String>());
		assert(workItem.getAttributesJson().length()>0);
		WorkItem workItem2 = new WorkItem(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, 1, 1, "test", workItem.getAttributesJson());
		assert(workItem2.getAttributes().size()==0);

	}
}
