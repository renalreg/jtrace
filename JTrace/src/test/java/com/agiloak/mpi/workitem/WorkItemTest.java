package com.agiloak.mpi.workitem;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;

public class WorkItemTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testCreate() throws MpiException {
		WorkItemManager wim = new WorkItemManager();
		wim.create(WorkItem.TYPE_DELINK_DUE_TO_CHANGED_DEMOG, 1, "test");
		assert(true);
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
