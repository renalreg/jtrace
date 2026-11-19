# PR 22 Follow-up Review Response

Response to `PR22 - 20261115.txt`.

The follow-up reviewer reports that the original six findings are resolved or answered, the PR's tests and public-API reproductions pass, and EXPLAIN confirms use of the source lookup index. These are external verification results, not tests executed in this workspace.

## 1. Original MRN At Storage Time

Confirmed in the checked-in sibling Data-Repository repository:

- `src/main/java/org/ukrdc/repository/UKRDCManager.java:44-45` constructs the EMPI Person and sets `unconsolidatedLocalId` from `patientRecord.getLocalPatientId()`. Lines 94-98 set the consolidated `localId` and store that same Person without changing the original MRN.
- `src/main/java/org/ukrdc/repository/pidmigrate/PidMigrationManager.java:155-163` reconstructs the Person with national identifiers restored, explicitly sets `unconsolidatedLocalId` from `oldPatientRecord.getLocalPatientId()`, then sets the consolidated `localId` and calls `store()`.

Both checked-in paths therefore meet the storage contract. No Repository changes are needed for this finding. Other external callers must also supply the original MRN; missing source metadata intentionally leaves the item open.

## 2. Message Text And Deployment Checks

The different Get/Set PID wording is intentional: the requested change was to include the validation work-item type. Checked-in Mirth exports in the sibling Data-Repository repository use status-based handling rather than matching the old validation message.

Deployed channel exports, alert rules and external monitoring are not available here, so their message dependencies still require a release-time check. The reviewer also notes that the concurrent index upgrade script was not exercised against existing data; that remains an environment-specific deployment check. Run `Database/SCRIPTS/1.4.6.SQL` outside a transaction.

## 3. Sequence-dependent Test

Accepted and fixed. `LinkRecordTest.testNoFindByPerson` now looks up `-1` instead of assuming generated person ID `99` is unused. The empty-result check uses a JUnit assertion, so it does not depend on Java assertions being enabled.

## Verification

Local compilation, database tests and concurrent index creation have not been run because Java, Maven and PostgreSQL tooling are unavailable on PATH. The unrelated baseline duplicate-master and timestamp test failures reported by the reviewer are not changed by this fix.
