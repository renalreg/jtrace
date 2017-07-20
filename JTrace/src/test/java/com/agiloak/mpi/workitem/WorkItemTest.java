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
		wim.create("test");
		assert(true);
	}

	@Test
	public void testCreateNull() throws MpiException {

		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);

		wim.create(null);

	}

	@Test
	public void testCreateEmpty() throws MpiException {

		WorkItemManager wim = new WorkItemManager();
		exception.expect(MpiException.class);

		wim.create("");

	}
}
