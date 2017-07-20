package com.agiloak.mpi.workitem;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.jtrace.JTraceException;

public class WorkItemTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testCreate() throws JTraceException {
		WorkItemManager wim = new WorkItemManager();
		wim.create("test");
		assert(true);
	}

	@Test
	public void testCreateNull() throws JTraceException {

		WorkItemManager wim = new WorkItemManager();
		exception.expect(JTraceException.class);

		wim.create(null);

	}

	@Test
	public void testCreateEmpty() throws JTraceException {

		WorkItemManager wim = new WorkItemManager();
		exception.expect(JTraceException.class);

		wim.create("");

	}
}
