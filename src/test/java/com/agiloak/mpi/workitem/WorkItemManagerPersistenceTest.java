package com.agiloak.mpi.workitem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.PidXREF;
import com.agiloak.mpi.index.persistence.PidXREFDAO;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class WorkItemManagerPersistenceTest {

	private Connection conn;
	private final WorkItemManager manager = new WorkItemManager();
	private static final int PERSON = 290001;
	private static final int TYPE = WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED;

	@BeforeClass
	public static void configure() throws Exception {
		SimpleConnectionManager.configure("postgres", "postgres", "localhost", "5432", "JTRACE");
	}

	@Before
	public void openConnection() throws Exception {
		conn = SimpleConnectionManager.getConnection();
	}

	@After
	public void closeConnection() throws Exception {
		if (conn != null) {
			try {
				conn.rollback();
			} finally {
				conn.close();
			}
		}
	}

	private Map<String, String> source(String sf, String se, String mrn) {
		Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("SF", sf);
		attributes.put("SE", se);
		attributes.put("MRN", mrn);
		return attributes;
	}

	@Test
	public void activeItemsDeduplicateButClosedItemsDoNot() throws Exception {
		WorkItem original = manager.create(conn, 4, PERSON, 1, "original");
		assertEquals(original.getId(), manager.create(conn, 4, PERSON, 1, "retry").getId());
		original.setStatus(WorkItemStatus.STATUS_WIP);
		WorkItemDAO.update(conn, original);
		assertEquals(original.getId(), manager.create(conn, 4, PERSON, 1, "retry").getId());
		original.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, original);
		WorkItem replacement = manager.create(conn, 4, PERSON, 1, "new condition");
		assertTrue(original.getId() != replacement.getId());
		assertEquals(WorkItemStatus.STATUS_OPEN, replacement.getStatus());
		assertEquals(2, manager.findByPerson(conn, PERSON).size());
	}

	@Test
	public void distinctTypesAndMastersRemainIndependentWhenUpdated() throws Exception {
		WorkItem first = manager.create(conn, 4, PERSON, 1, "first");
		WorkItem otherType = manager.create(conn, 5, PERSON, 1, "other type");
		WorkItem otherMaster = manager.create(conn, 4, PERSON, 2, "other master");
		assertEquals(3, manager.findByPerson(conn, PERSON).size());
		assertEquals(first.getId(), manager.create(conn, 4, PERSON, 1, "retry").getId());
		first.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, first);
		assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, otherType.getId()).getStatus());
		assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, otherMaster.getId()).getStatus());
		assertNotNull(WorkItemDAO.findByPersonAndMaster(conn, PERSON, 1));
	}

	@Test
	public void typeNineRequiresAllSourceAttributesToMatchStrictly() throws Exception {
		WorkItem first = manager.create(conn, TYPE, PERSON, 1, "first", source("A", "B", "001"));
		assertEquals(first.getId(), manager.create(conn, TYPE, PERSON, 1, "retry", source("A", "B", "001")).getId());
		first.setStatus(WorkItemStatus.STATUS_WIP);
		WorkItemDAO.update(conn, first);
		assertEquals(first.getId(), manager.create(conn, TYPE, PERSON, 1, "retry WIP", source("A", "B", "001")).getId());
		manager.create(conn, TYPE, PERSON, 1, "different facility", source("a", "B", "001"));
		manager.create(conn, TYPE, PERSON, 1, "different extract", source("A", "C", "001"));
		manager.create(conn, TYPE, PERSON, 1, "different MRN", source("A", "B", "1"));
		for (String missing : new String[] { "SF", "SE", "MRN" }) {
			Map<String, String> attributes = source("A", "B", "001");
			attributes.remove(missing);
			WorkItem incomplete = manager.create(conn, TYPE, PERSON, 1, "incomplete", attributes);
			assertTrue(incomplete.getId() != manager.create(conn, TYPE, PERSON, 1, "retry incomplete", attributes).getId());
			attributes.put(missing, null);
			assertTrue(first.getId() != manager.create(conn, TYPE, PERSON, 1, "null source", attributes).getId());
		}
		WorkItem noAttributes = manager.create(conn, TYPE, PERSON, 1, "no attributes", (Map<String, String>) null);
		assertTrue(noAttributes.getId() != manager.create(conn, TYPE, PERSON, 1, "retry no attributes").getId());
		assertEquals(0, manager.findBySource(conn, "A", "B", "001").size());
		assertEquals(0, manager.findBySource(conn, null, "B", "001").size());
		assertEquals(0, manager.findBySource(conn, "A", null, "001").size());
		assertEquals(0, manager.findBySource(conn, "A", "B", null).size());
		first.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, first);
		WorkItem replacement = manager.create(conn, TYPE, PERSON, 1, "new condition", source("A", "B", "001"));
		assertTrue(first.getId() != replacement.getId());
		assertEquals(1, manager.findBySource(conn, "A", "B", "001").size());
		assertEquals(replacement.getId(), manager.findBySource(conn, "A", "B", "001").get(0).getId());
	}

	@Test(expected = MpiException.class)
	public void automaticClosureRejectsAutoCommitConnections() throws Exception {
		conn.setAutoCommit(true);
		try {
			manager.closeAutomatically(conn, Integer.MAX_VALUE);
		} finally {
			conn.setAutoCommit(false);
		}
	}

	@Test
	public void sourceLookupIncludesPlaceholderAndActualPersonsButOnlyOpenTypeNine() throws Exception {
		Map<String, String> attributes = source("WORKITEM_TEST", "SOURCE", "290001");
		WorkItem placeholder = manager.create(conn, TYPE, -1, -1, "placeholder", attributes);
		WorkItem actual = manager.create(conn, TYPE, PERSON, 1, "actual", attributes);
		WorkItem closed = manager.create(conn, TYPE, PERSON, 2, "closed", attributes);
		closed.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, closed);
		WorkItem wip = manager.create(conn, TYPE, PERSON, 3, "wip", attributes);
		wip.setStatus(WorkItemStatus.STATUS_WIP);
		WorkItemDAO.update(conn, wip);
		manager.create(conn, 4, PERSON, 1, "other type", attributes);
		List<WorkItem> found = manager.findBySource(conn, "WORKITEM_TEST", "SOURCE", "290001");
		assertEquals(2, found.size());
		Map<Integer, WorkItem> byId = new HashMap<Integer, WorkItem>();
		for (WorkItem item : found) {
			byId.put(item.getId(), item);
		}
		assertEquals(-1, byId.get(placeholder.getId()).getPersonId());
		assertEquals(PERSON, byId.get(actual.getId()).getPersonId());
		assertEquals(WorkItemStatus.STATUS_OPEN, byId.get(actual.getId()).getStatus());
	}

	@Test
	public void durableLookupUsesPersistedPidAndExactSourceWithoutPersonState() throws Exception {
		String pid = "9200000001";
		String mrn = "00290001";
		assertTrue(PidXREFDAO.create(conn, new PidXREF("WILOOK", "A", mrn).setPid(pid)));
		assertTrue(PidXREFDAO.create(conn, new PidXREF("WILKB", "B", mrn).setPid(pid)));
		assertTrue(PidXREFDAO.create(conn, new PidXREF("WILOOK", "A", "290001").setPid(pid)));
		assertTrue(PidXREFDAO.create(conn, new PidXREF("WILKOTH", "A", mrn).setPid("9200000002")));
		Map<String, String> attributes = source("WILOOK", "A", mrn);
		WorkItem placeholder = manager.create(conn, TYPE, -1, -1, "placeholder", attributes);
		WorkItem actual = manager.create(conn, TYPE, PERSON, 1, "actual", source("WILKB", "B", mrn));
		WorkItem closed = manager.create(conn, TYPE, PERSON, 2, "closed", attributes);
		closed.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, closed);
		WorkItem wip = manager.create(conn, TYPE, PERSON, 3, "wip", attributes);
		wip.setStatus(WorkItemStatus.STATUS_WIP);
		WorkItemDAO.update(conn, wip);
		WorkItem otherMrn = manager.create(conn, TYPE, PERSON, 1, "other MRN", source("WILOOK", "A", "290001"));
		manager.create(conn, TYPE, PERSON, 1, "other PID", source("WILKOTH", "A", mrn));
		manager.create(conn, TYPE, PERSON, 1, "wrong facility", source("wilook", "A", mrn));
		manager.create(conn, TYPE, PERSON, 1, "wrong extract", source("WILOOK", "B", mrn));
		manager.create(conn, TYPE, PERSON, 1, "missing extract", source("WILOOK", null, mrn));
		manager.create(conn, 4, PERSON, 1, "other type", attributes);

		List<WorkItem> found = new WorkItemManager().findOpenByPidAndLocalId(conn, pid, mrn);
		assertEquals(2, found.size());
		Map<Integer, WorkItem> byId = new HashMap<Integer, WorkItem>();
		for (WorkItem item : found) {
			byId.put(item.getId(), item);
			assertEquals(WorkItemStatus.STATUS_OPEN, item.getStatus());
			assertEquals(TYPE, item.getType());
		}
		assertEquals(-1, byId.get(placeholder.getId()).getPersonId());
		assertEquals(PERSON, byId.get(actual.getId()).getPersonId());
		assertEquals(placeholder.getAttributes(), byId.get(placeholder.getId()).getAttributes());
		assertEquals(placeholder.getCreationDate(), byId.get(placeholder.getId()).getCreationDate());
		assertEquals(0, manager.findOpenByPidAndLocalId(conn, "9200000003", mrn).size());
		assertEquals(0, manager.findOpenByPidAndLocalId(conn, pid, "unmapped").size());
		assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, otherMrn.getId()).getStatus());
		assertEquals(WorkItemStatus.STATUS_WIP, WorkItemDAO.get(conn, wip.getId()).getStatus());
		assertEquals(WorkItemStatus.STATUS_CLOSED, WorkItemDAO.get(conn, closed.getId()).getStatus());
	}

	@Test
	public void durableLookupReturnsNothingForMissingArgumentsWithoutQuerying() throws Exception {
		assertTrue(manager.findOpenByPidAndLocalId(null, null, "MRN").isEmpty());
		assertTrue(manager.findOpenByPidAndLocalId(null, "", "MRN").isEmpty());
		assertTrue(manager.findOpenByPidAndLocalId(null, "PID", null).isEmpty());
		assertTrue(manager.findOpenByPidAndLocalId(null, "PID", "").isEmpty());
	}

	@Test
	public void automaticClosureIsOpenOnlyAuditedIdempotentAndRollsBack() throws Exception {
		WorkItem open = manager.create(conn, 4, PERSON, 1, "open");
		WorkItem wip = manager.create(conn, 5, PERSON, 1, "wip");
		WorkItem closed = manager.create(conn, 6, PERSON, 1, "closed");
		wip.setStatus(WorkItemStatus.STATUS_WIP);
		wip.setUpdatedBy("MANUAL");
		wip.setUpdateDesc("Investigating");
		WorkItemDAO.update(conn, wip);
		closed.setStatus(WorkItemStatus.STATUS_CLOSED);
		WorkItemDAO.update(conn, closed);
		int auditCount = AuditDAO.findByPerson(conn, PERSON).size();
		java.sql.Savepoint beforeClosure = conn.setSavepoint();
		manager.closeAutomatically(conn, open.getId());
		manager.closeAutomatically(conn, open.getId());
		manager.closeAutomatically(conn, wip.getId());
		manager.closeAutomatically(conn, closed.getId());
		manager.closeAutomatically(conn, Integer.MAX_VALUE);
		WorkItem updated = WorkItemDAO.get(conn, open.getId());
		assertEquals(WorkItemStatus.STATUS_CLOSED, updated.getStatus());
		assertEquals("JTRACE_AUTO", updated.getUpdatedBy());
		assertEquals("Closed automatically after successful reprocessing; original condition no longer applies.", updated.getUpdateDesc());
		assertTrue(updated.getUpdateDesc().length() <= 100);
		assertEquals(WorkItemStatus.STATUS_WIP, WorkItemDAO.get(conn, wip.getId()).getStatus());
		assertEquals("MANUAL", WorkItemDAO.get(conn, wip.getId()).getUpdatedBy());
		assertEquals(wip.getLastUpdated(), WorkItemDAO.get(conn, wip.getId()).getLastUpdated());
		assertEquals(closed.getLastUpdated(), WorkItemDAO.get(conn, closed.getId()).getLastUpdated());
		List<Audit> audits = AuditDAO.findByPerson(conn, PERSON);
		assertEquals(auditCount + 1, audits.size());
		Audit closureAudit = null;
		for (Audit audit : audits) {
			if (Integer.toString(open.getId()).equals(audit.getAttributes().get("Id"))) {
				closureAudit = audit;
			}
		}
		assertNotNull(closureAudit);
		assertEquals(Audit.WORK_ITEM_UPDATED, closureAudit.getType());
		assertEquals("JTRACE_AUTO", closureAudit.getUpdatedBy());
		assertEquals(updated.getUpdateDesc(), closureAudit.getAttributes().get("UpdateDesc"));
		assertEquals(Integer.toString(WorkItemStatus.STATUS_CLOSED), closureAudit.getAttributes().get("Status"));
		conn.rollback(beforeClosure);
		assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, open.getId()).getStatus());
		assertEquals(auditCount, AuditDAO.findByPerson(conn, PERSON).size());
	}

	@Test
	public void automaticClosureWaitsForConcurrentManualWipAndRechecksStatus() throws Exception {
		final int person = 290002;
		final WorkItem item = manager.create(conn, 4, person, 1, "concurrent");
		conn.commit();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		final CountDownLatch ready = new CountDownLatch(1);
		try {
			item.setStatus(WorkItemStatus.STATUS_WIP);
			item.setUpdatedBy("MANUAL");
			WorkItemDAO.update(conn, item);
			Future<?> closure = executor.submit(() -> {
				try (Connection automatic = SimpleConnectionManager.getConnection()) {
					try {
						ready.countDown();
						manager.closeAutomatically(automatic, item.getId());
						automatic.commit();
					} finally {
						automatic.rollback();
					}
				}
				return null;
			});
			assertTrue(ready.await(5, TimeUnit.SECONDS));
			try {
				closure.get(200, TimeUnit.MILLISECONDS);
				org.junit.Assert.fail("Closure must wait for the manual transaction's row lock");
			} catch (TimeoutException expected) {
				// The manual transaction still holds the row lock.
			}
			conn.commit();
			closure.get(5, TimeUnit.SECONDS);
			assertEquals(WorkItemStatus.STATUS_WIP, WorkItemDAO.get(conn, item.getId()).getStatus());
			assertEquals("MANUAL", WorkItemDAO.get(conn, item.getId()).getUpdatedBy());
			assertEquals(0, AuditDAO.findByPerson(conn, person).size());
		} finally {
			conn.rollback();
			executor.shutdown();
			assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
			WorkItemDAO.deleteByPerson(conn, person);
			AuditDAO.deleteByPerson(conn, person);
			conn.commit();
		}
	}
}
