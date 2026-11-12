package com.agiloak.mpi.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.index.persistence.PidXREFDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.WorkItemManager;
import com.agiloak.mpi.workitem.WorkItemStatus;
import com.agiloak.mpi.workitem.WorkItemType;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerConditionAwareClosureTest {

    private Connection conn;
    private final UKRDCIndexManager im = new UKRDCIndexManager();
    private final WorkItemManager workItems = new WorkItemManager();
    private static final String FACILITY = "CLOSURE_TEST";
    private static final String EXTRACT = "SOURCE";

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

    private Person person(String localId) {
        Person person = new Person().setOriginator(FACILITY).setLocalIdType("MR")
                .setLocalId(localId).setGivenName("ALICE").setSurname("JONES")
                .setGender("2").setDateOfBirth(Date.valueOf("1980-02-03"))
                .setEffectiveDate(Date.valueOf("2025-01-01"));
        person.setUnconsolidatedLocalId(localId);
        return person;
    }

    private Person storedPerson(String localId) throws Exception {
        Person person = person(localId);
        assertTrue(PersonDAO.create(conn, person));
        return person;
    }

    private MasterRecord master(String type, String id) throws Exception {
        MasterRecord master = new MasterRecord(person("demographics"))
                .setNationalIdType(type).setNationalId(id);
        assertTrue(MasterRecordDAO.create(conn, master));
        return master;
    }

    private void link(Person person, MasterRecord master) throws Exception {
        LinkRecordDAO.create(conn, new LinkRecord(master.getId(), person.getId()));
    }

    private WorkItem sourceItem(int personId, int masterId, String facility,
            String extract, String mrn) throws Exception {
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put("SF", facility);
        attributes.put("SE", extract);
        attributes.put("MRN", mrn);
        return workItems.create(conn, WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED,
                personId, masterId, "Source demographics mismatch", attributes);
    }

    @Test
    public void multipleNationalIdsCorroboratingOneMasterResolveOldTypeTen() throws Exception {
        Person stored = storedPerson("inbound");
        Person peer = storedPerson("peer");
        MasterRecord ukrdc = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_1");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        MasterRecord chi = master("CHI", "CLOSURE_CHI_1");
        link(peer, ukrdc);
        link(peer, nhs);
        link(peer, chi);
        link(stored, nhs);
        link(stored, chi);
        Person inbound = person("inbound").addNationalId(nhs.getNationalIdentity())
                .addNationalId(chi.getNationalIdentity());
        WorkItem old = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                stored.getId(), ukrdc.getId(), "Historical duplicate candidate count");

        assertEquals(Collections.singletonList(old.getId()), im.findResolvedWorkItems(conn, inbound));
        assertEquals("Planning must not close the item", WorkItemStatus.STATUS_OPEN,
                WorkItemDAO.get(conn, old.getId()).getStatus());
    }

    @Test
    public void distinctVerifiedCandidatesKeepTypeTenOpen() throws Exception {
        Person stored = storedPerson("inbound");
        Person first = storedPerson("first");
        Person second = storedPerson("second");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        MasterRecord chi = master("CHI", "CLOSURE_CHI_1");
        MasterRecord firstUkrdc = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_1");
        MasterRecord secondUkrdc = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_2");
        link(first, nhs);
        link(first, firstUkrdc);
        link(second, chi);
        link(second, secondUkrdc);
        Person inbound = person("inbound").addNationalId(nhs.getNationalIdentity())
                .addNationalId(chi.getNationalIdentity());
        WorkItem item = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                stored.getId(), firstUkrdc.getId(), "Two distinct verified candidates");

        assertTrue(im.verifyMatch(inbound, firstUkrdc));
        assertTrue(im.verifyMatch(inbound, secondUkrdc));
        assertTrue(im.findResolvedWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());
    }

    @Test
    public void mismatchingDemographicsKeepTypeFourActive() throws Exception {
        Person stored = storedPerson("inbound");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        link(stored, nhs);
        WorkItem item = workItems.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,
                stored.getId(), nhs.getId(), "Demographics still disagree");
        // Change every DOB component: exact DOB alone is sufficient for verification.
        Person inbound = person("inbound").setDateOfBirth(Date.valueOf("1991-07-19"));

        assertFalse(im.verifyMatch(inbound, nhs));
        assertTrue(im.findResolvedWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());
    }

    @Test
    public void correctedDemographicsPlanTypeFourClosureWithoutChangingMaster() throws Exception {
        Person stored = storedPerson("inbound");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        link(stored, nhs);
        WorkItem item = workItems.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL,
                stored.getId(), nhs.getId(), "Previous demographics disagreed");

        assertEquals(Collections.singletonList(item.getId()),
                im.findResolvedWorkItems(conn, person("inbound")));
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());
        assertEquals(nhs.getDateOfBirth(), MasterRecordDAO.get(conn, nhs.getId()).getDateOfBirth());
    }

    @Test
    public void placeholderClosureIsSourceIsolatedAndDeferredUntilSuccessfulStore() throws Exception {
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        Person inbound = person("consolidated");
        inbound.setUnconsolidatedLocalId("001");
        WorkItem matching = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "001");
        WorkItem otherFacility = sourceItem(999999999, nhs.getId(), "OTHER", EXTRACT, "001");
        WorkItem otherExtract = sourceItem(999999999, nhs.getId(), FACILITY, "OTHER", "001");
        WorkItem otherMrn = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "1");
        WorkItem consolidatedMrn = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "consolidated");
        MasterRecord otherMaster = master("CHI", "CLOSURE_CHI_1");
        WorkItem wip = sourceItem(999999999, otherMaster.getId(), FACILITY, EXTRACT, "001");
        wip.setStatus(WorkItemStatus.STATUS_WIP);
        WorkItemDAO.update(conn, wip);

        im.stageSourceWorkItemClosures(conn, inbound, FACILITY, EXTRACT);

        assertEquals(Collections.singletonList(matching.getId()), inbound.getResolvedValidationWorkItemIds());
        assertTrue(person("another").getResolvedValidationWorkItemIds().isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, matching.getId()).getStatus());
        UKRDCIndexManagerResponse response = new StoreCommand(inbound).execute(im, conn);
        assertEquals(UKRDCIndexManagerResponse.SUCCESS, response.getStatus());
        assertEquals(WorkItemStatus.STATUS_CLOSED, WorkItemDAO.get(conn, matching.getId()).getStatus());
        for (WorkItem unrelated : Arrays.asList(otherFacility, otherExtract, otherMrn, consolidatedMrn)) {
            assertEquals("Other sources must remain open", WorkItemStatus.STATUS_OPEN,
                    WorkItemDAO.get(conn, unrelated.getId()).getStatus());
        }
        assertEquals(WorkItemStatus.STATUS_WIP, WorkItemDAO.get(conn, wip.getId()).getStatus());
    }

    @Test
    public void successfulSecondNationalIdCannotMaskFirstMismatchUntilDemographicsAreCorrected() throws Exception {
        String facility = "CLSRNI";
        String extract = "NITEST";
        Person badCandidate = person("9900000001").setOriginator(facility)
                .setDateOfBirth(Date.valueOf("1991-07-19"))
                .setGivenName("ROBERT").setSurname("SMITH");
        Person goodCandidate = person("9900000002").setOriginator(facility);
        assertTrue(PersonDAO.create(conn, badCandidate));
        assertTrue(PersonDAO.create(conn, goodCandidate));
        MasterRecord badNi = master("NHS", "CLOSURE_BAD_NI");
        MasterRecord goodNi = master("CHI", "CLOSURE_GOOD_NI");
        link(badCandidate, badNi);
        link(goodCandidate, goodNi);
        // The lookup joins px.pid to person.localId, not to person.id.
        assertTrue(PidXREFDAO.create(conn, new PidXREF(facility, extract, "old_bad_mrn")
                .setPid(badCandidate.getLocalId())));
        assertTrue(PidXREFDAO.create(conn, new PidXREF(facility, extract, "old_good_mrn")
                .setPid(goodCandidate.getLocalId())));
        Person inbound = person("new_ni_mrn").setOriginator(facility)
                .addNationalId(badNi.getNationalIdentity()).addNationalId(goodNi.getNationalIdentity());

        assertEquals(goodCandidate.getLocalId(), im.getLocalPIDInternal(conn, inbound, facility, extract));
        List<WorkItem> items = workItems.findBySource(conn, facility, extract,
                inbound.getUnconsolidatedLocalId());
        assertEquals(1, items.size());
        WorkItem badItem = items.get(0);
        assertEquals(WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED, badItem.getType());
        assertEquals(badNi.getId(), badItem.getMasterId());
        assertEquals(999999999, badItem.getPersonId());

        im.stageSourceWorkItemClosures(conn, inbound, facility, extract);
        assertTrue("A successful second NI must not resolve the first NI's mismatch",
                inbound.getResolvedValidationWorkItemIds().isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, badItem.getId()).getStatus());

        badCandidate.setDateOfBirth(inbound.getDateOfBirth());
        PersonDAO.update(conn, badCandidate);
        // Put the successful NI first to catch closure checks that stop at any match.
        Collections.reverse(inbound.getNationalIds());
        im.stageSourceWorkItemClosures(conn, inbound, facility, extract);
        assertTrue("Matching DOB must not mask the remaining strict name mismatch",
                inbound.getResolvedValidationWorkItemIds().isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, badItem.getId()).getStatus());

        badCandidate.setGivenName(inbound.getGivenName()).setSurname(inbound.getSurname());
        PersonDAO.update(conn, badCandidate);
        im.stageSourceWorkItemClosures(conn, inbound, facility, extract);
        assertEquals("The same old item should stage once its own NI candidates match strictly",
                Collections.singletonList(badItem.getId()), inbound.getResolvedValidationWorkItemIds());
        assertEquals("Staging alone must not close the corrected item", WorkItemStatus.STATUS_OPEN,
                WorkItemDAO.get(conn, badItem.getId()).getStatus());
    }

    @Test
    public void failedStoreDoesNotClosePlannedOrStagedItems() throws Exception {
        Person stored = storedPerson("inbound");
        WorkItem planned = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                stored.getId(), -1, "No remaining inferred candidates");
        WorkItem staged = sourceItem(999999999, -1, FACILITY, EXTRACT, "inbound");
        Person inbound = person("inbound");
        im.stageSourceWorkItemClosures(conn, inbound, FACILITY, EXTRACT);
        assertEquals(Collections.singletonList(planned.getId()), im.findResolvedWorkItems(conn, inbound));
        assertEquals(Collections.singletonList(staged.getId()), inbound.getResolvedValidationWorkItemIds());
        UKRDCIndexManager failing = new UKRDCIndexManager() {
            @Override
            protected NationalIdentity createOrUpdate(Connection connection, Person person) throws MpiException {
                super.createOrUpdate(connection, person);
                throw new MpiException("Injected failure after storage writes");
            }
        };
        Savepoint beforeStore = conn.setSavepoint();
        try {
            new StoreCommand(inbound).execute(failing, conn);
            fail("Store must propagate the injected failure");
        } catch (MpiException expected) {
            assertEquals("Injected failure after storage writes", expected.getMessage());
        }
        // Check before rollback as well, so rollback cannot mask premature closure.
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, planned.getId()).getStatus());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, staged.getId()).getStatus());
        conn.rollback(beforeStore);
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, planned.getId()).getStatus());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, staged.getId()).getStatus());
    }
}
