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
    private static final String FACILITY = "CLSR";
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
        link(stored, ukrdc);
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
    public void claimedLinksStayOpenWhenMasterReflectsInboundButPeerStillConflicts() throws Exception {
        for (int type : Arrays.asList(WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY,
                WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL)) {
            Person stored = storedPerson("claim" + type);
            Person peer = person("peer" + type).setDateOfBirth(Date.valueOf("1991-07-19"))
                    .setGivenName("ROBERT").setSurname("SMITH");
            assertTrue(PersonDAO.create(conn, peer));
            MasterRecord claimed = master(type == WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY
                    ? NationalIdentity.UKRDC_TYPE : "NHS", "CLOSURE_CLAIM_" + type);
            link(stored, claimed);
            link(peer, claimed);
            WorkItem item = workItems.create(conn, type, stored.getId(), claimed.getId(),
                    "Master already overwritten by B; peer A still disagrees");
            Person inbound = person(stored.getLocalId());

            assertTrue("The overwritten master alone must not justify closure", im.verifyMatch(inbound, claimed));
            assertFalse(im.verifyMatch(inbound, new MasterRecord(peer)));
            assertFalse(im.findResolvedWorkItems(conn, inbound).contains(item.getId()));
            assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());

            peer.setDateOfBirth(inbound.getDateOfBirth()).setGivenName(inbound.getGivenName())
                    .setSurname(inbound.getSurname());
            PersonDAO.update(conn, peer);
            assertEquals(Collections.singletonList(item.getId()), im.findResolvedWorkItems(conn, inbound));
            assertEquals("Planning is read-only", WorkItemStatus.STATUS_OPEN,
                    WorkItemDAO.get(conn, item.getId()).getStatus());
        }
    }

    @Test
    public void verifiedTypeThreeCandidateRequiresActualLinkOrCandidateRemoval() throws Exception {
        Person stored = storedPerson("inbound");
        Person peer = storedPerson("peer");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        MasterRecord candidate = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_1");
        link(peer, nhs);
        link(peer, candidate);
        Person inbound = person("inbound").addNationalId(nhs.getNationalIdentity());
        WorkItem item = workItems.create(conn, WorkItemType.TYPE_INFERRED_LINK_NOT_VERIFIED_PRIMARY,
                stored.getId(), candidate.getId(), "Corrected demographics but not linked");

        assertTrue(im.verifyMatch(inbound, candidate));
        assertTrue(im.findResolvedWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());
        assertEquals("An identifier no longer claimed removes the inferred candidate",
                Collections.singletonList(item.getId()), im.findResolvedWorkItems(conn, person("inbound")));

        link(stored, candidate);
        assertEquals(Collections.singletonList(item.getId()), im.findResolvedWorkItems(conn, inbound));
    }

    @Test
    public void soleVerifiedTypeTenCandidateMustBeCurrentLinkedMaster() throws Exception {
        Person stored = storedPerson("inbound");
        Person peer = storedPerson("peer");
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        MasterRecord candidate = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_1");
        MasterRecord current = master(NationalIdentity.UKRDC_TYPE, "CLOSURE_UKRDC_2");
        link(peer, nhs);
        link(peer, candidate);
        WorkItem item = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                stored.getId(), candidate.getId(), "Only one verified candidate, not yet linked");
        Person inbound = person("inbound").addNationalId(nhs.getNationalIdentity());

        assertTrue(im.verifyMatch(inbound, candidate));
        assertTrue("No current link is not resolution", im.findResolvedWorkItems(conn, inbound).isEmpty());
        link(stored, current);
        assertTrue("A different current UKRDC master is not resolution",
                im.findResolvedWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());

        Person unverified = person("inbound").setDateOfBirth(Date.valueOf("1991-07-19"))
                .setGivenName("ROBERT").setSurname("SMITH").addNationalId(nhs.getNationalIdentity());
        assertFalse(im.verifyMatch(unverified, candidate));
        assertEquals("No verified candidates resolves the multiple-match condition",
                Collections.singletonList(item.getId()), im.findResolvedWorkItems(conn, unverified));

        LinkRecordDAO.delete(conn, new LinkRecord(current.getId(), stored.getId()));
        link(stored, candidate);
        assertEquals(Collections.singletonList(item.getId()), im.findResolvedWorkItems(conn, inbound));
    }

    @Test
    public void persistedMappingClosesPlaceholderOnFreshStoreWithoutPidCommands() throws Exception {
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        Person inbound = person("9000001");
        inbound.setUnconsolidatedLocalId("001");
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "001")
                .setPid(inbound.getLocalId())));
        WorkItem matching = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "001");
        WorkItem otherFacility = sourceItem(999999999, nhs.getId(), "OTHER", EXTRACT, "001");
        WorkItem otherExtract = sourceItem(999999999, nhs.getId(), FACILITY, "OTHER", "001");
        WorkItem otherMrn = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "1");
        WorkItem consolidatedMrn = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "9000001");
        assertTrue(PidXREFDAO.create(conn, new PidXREF("OTHER", EXTRACT, "001").setPid("9000002")));
        MasterRecord otherMaster = master("CHI", "CLOSURE_CHI_1");
        WorkItem wip = sourceItem(999999999, otherMaster.getId(), FACILITY, EXTRACT, "001");
        wip.setStatus(WorkItemStatus.STATUS_WIP);
        WorkItemDAO.update(conn, wip);
        WorkItem closed = sourceItem(999999999, -1, FACILITY, EXTRACT, "001");
        closed.setStatus(WorkItemStatus.STATUS_CLOSED);
        WorkItemDAO.update(conn, closed);
        WorkItem otherType = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                999999999, nhs.getId(), "Same source attributes, different type", matching.getAttributes());

        assertEquals(Collections.singletonList(matching.getId()), im.findResolvedSourceWorkItems(conn, inbound));
        assertTrue(im.findResolvedSourceWorkItems(conn, person("9000002")).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, matching.getId()).getStatus());
        Person reconstructed = person("9000001");
        reconstructed.setUnconsolidatedLocalId("001");
        UKRDCIndexManagerResponse response = new StoreCommand(reconstructed).execute(im, conn);
        assertEquals(UKRDCIndexManagerResponse.SUCCESS, response.getStatus());
        assertEquals(WorkItemStatus.STATUS_CLOSED, WorkItemDAO.get(conn, matching.getId()).getStatus());
        for (WorkItem unrelated : Arrays.asList(otherFacility, otherExtract, otherMrn, consolidatedMrn, otherType)) {
            assertEquals("Other sources must remain open", WorkItemStatus.STATUS_OPEN,
                    WorkItemDAO.get(conn, unrelated.getId()).getStatus());
        }
        assertEquals(WorkItemStatus.STATUS_WIP, WorkItemDAO.get(conn, wip.getId()).getStatus());
        assertEquals(WorkItemStatus.STATUS_CLOSED, WorkItemDAO.get(conn, closed.getId()).getStatus());
    }

    @Test
    public void sourceClosureRequiresMappingForBothFinalPidAndOriginalMrn() throws Exception {
        WorkItem item = sourceItem(999999999, -1, FACILITY, EXTRACT, "001");
        Person inbound = person("9000001");
        inbound.setUnconsolidatedLocalId("001");
        assertTrue(im.findResolvedSourceWorkItems(conn, inbound).isEmpty());
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "001").setPid("9000002")));
        assertTrue("Matching source with another PID must not close", im.findResolvedSourceWorkItems(conn, inbound).isEmpty());
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "002").setPid("9000001")));
        assertTrue("Matching PID with another MRN must not close", im.findResolvedSourceWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, item.getId()).getStatus());
    }

    @Test
    public void restoredFinalIdentifiersKeepSourceMismatchOpenBeforeStoreOverwritesCandidate() throws Exception {
        Person stored = person("9000001").setDateOfBirth(Date.valueOf("1991-07-19"))
                .setGivenName("ROBERT").setSurname("SMITH");
        assertTrue(PersonDAO.create(conn, stored));
        MasterRecord nhs = master("NHS", "CLOSURE_NHS_1");
        link(stored, nhs);
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "old_mrn").setPid("9000001")));
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "new_mrn").setPid("9000001")));
        WorkItem item = sourceItem(999999999, nhs.getId(), FACILITY, EXTRACT, "new_mrn");

        Person migration = person("9000001");
        migration.setUnconsolidatedLocalId("new_mrn");
        assertEquals("Temporarily removed identifiers are not the final store input",
                Collections.singletonList(item.getId()), im.findResolvedSourceWorkItems(conn, migration));

        Person finalPerson = person("9000001").addNationalId(nhs.getNationalIdentity());
        finalPerson.setUnconsolidatedLocalId("new_mrn");
        assertTrue("Restored NI still identifies a conflicting persisted candidate",
                im.findResolvedSourceWorkItems(conn, finalPerson).isEmpty());
        assertEquals(UKRDCIndexManagerResponse.SUCCESS, new StoreCommand(finalPerson).execute(im, conn).getStatus());
        assertEquals("Storage must not erase the evidence before deciding closure", WorkItemStatus.STATUS_OPEN,
                WorkItemDAO.get(conn, item.getId()).getStatus());
        assertEquals("After storage the candidate matches, demonstrating why planning must precede mutation",
                Collections.singletonList(item.getId()), im.findResolvedSourceWorkItems(conn, finalPerson));
    }

    @Test
    public void successfulSecondNationalIdCannotMaskFirstMismatchUntilDemographicsAreCorrected() throws Exception {
        String facility = "CLSRNI";
        String extract = "NITEST";
        Person badCandidate = person("9900001").setOriginator(facility)
                .setDateOfBirth(Date.valueOf("1991-07-19"))
                .setGivenName("ROBERT").setSurname("SMITH");
        Person goodCandidate = person("9900002").setOriginator(facility);
        assertTrue(PersonDAO.create(conn, badCandidate));
        assertTrue(PersonDAO.create(conn, goodCandidate));
        MasterRecord badNi = master("NHS", "CLOSURE_BAD_NI");
        MasterRecord goodNi = master("CHI", "CLOSURE_GOOD_NI");
        link(badCandidate, badNi);
        link(goodCandidate, goodNi);
        // The lookup joins px.pid to person.localId, not to person.id.
        assertTrue(PidXREFDAO.create(conn, new PidXREF(facility, extract, "old_bad")
                .setPid(badCandidate.getLocalId())));
        assertTrue(PidXREFDAO.create(conn, new PidXREF(facility, extract, "old_good")
                .setPid(goodCandidate.getLocalId())));
        Person inbound = person("9900003").setOriginator(facility)
                .addNationalId(badNi.getNationalIdentity()).addNationalId(goodNi.getNationalIdentity());
        inbound.setUnconsolidatedLocalId("new_ni_mrn");

        assertEquals(goodCandidate.getLocalId(), im.getLocalPIDInternal(conn, inbound, facility, extract));
        List<WorkItem> items = workItems.findBySource(conn, facility, extract,
                inbound.getUnconsolidatedLocalId());
        assertEquals(1, items.size());
        WorkItem badItem = items.get(0);
        assertEquals(WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED, badItem.getType());
        assertEquals(badNi.getId(), badItem.getMasterId());
        assertEquals(999999999, badItem.getPersonId());
        assertEquals("new_ni_mrn", badItem.getAttributes().get("MRN"));
        assertTrue(workItems.findBySource(conn, facility, extract, inbound.getLocalId()).isEmpty());
        assertTrue(PidXREFDAO.create(conn, new PidXREF(facility, extract, "new_ni_mrn")
                .setPid(goodCandidate.getLocalId())));
        inbound.setLocalId(goodCandidate.getLocalId());

        assertTrue("A successful second NI must not resolve the first NI's mismatch",
                im.findResolvedSourceWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, badItem.getId()).getStatus());

        badCandidate.setDateOfBirth(inbound.getDateOfBirth());
        PersonDAO.update(conn, badCandidate);
        // Put the successful NI first to catch closure checks that stop at any match.
        Collections.reverse(inbound.getNationalIds());
        assertTrue("Matching DOB must not mask the remaining strict name mismatch",
                im.findResolvedSourceWorkItems(conn, inbound).isEmpty());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, badItem.getId()).getStatus());

        badCandidate.setGivenName(inbound.getGivenName()).setSurname(inbound.getSurname());
        PersonDAO.update(conn, badCandidate);
        assertEquals("The same old item resolves once its own NI candidates match strictly",
                Collections.singletonList(badItem.getId()), im.findResolvedSourceWorkItems(conn, inbound));
        assertEquals("Planning alone must not close the corrected item", WorkItemStatus.STATUS_OPEN,
                WorkItemDAO.get(conn, badItem.getId()).getStatus());
    }

    @Test
    public void failedStoreDoesNotClosePersonOrSourceItems() throws Exception {
        Person stored = storedPerson("inbound");
        WorkItem planned = workItems.create(conn, WorkItemType.TYPE_MULTIPLE_UKRDC_MATCH,
                stored.getId(), -1, "No remaining inferred candidates");
        WorkItem source = sourceItem(999999999, -1, FACILITY, EXTRACT, "inbound");
        Person inbound = person("inbound");
        assertTrue(PidXREFDAO.create(conn, new PidXREF(FACILITY, EXTRACT, "inbound").setPid("inbound")));
        assertEquals(Collections.singletonList(planned.getId()), im.findResolvedWorkItems(conn, inbound));
        assertEquals(Collections.singletonList(source.getId()), im.findResolvedSourceWorkItems(conn, inbound));
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
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, source.getId()).getStatus());
        conn.rollback(beforeStore);
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, planned.getId()).getStatus());
        assertEquals(WorkItemStatus.STATUS_OPEN, WorkItemDAO.get(conn, source.getId()).getStatus());
    }
}
