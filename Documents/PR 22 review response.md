# PR 22 Review Response

Response to `PR 22 - 20261113.txt`. Findings were checked against the source; the fixes and regression tests below have not been executed locally because Java, Maven and PostgreSQL tooling are unavailable on PATH.

## 1. Claimed Links Closing After Master Overwrite

Accepted. For types 2 and 4, matching the current master alone was insufficient: a previous failed claim could have replaced that master's demographics. Automatic closure now also requires verification against every other person linked to that master. Tests cover conflicting peers and corrected agreement for both types.

## 2. Closing Without Actual Link Resolution

Accepted, including the type 10 concern. Type 3 now requires that a verified candidate is actually linked to the person, unless it is no longer an inferred candidate. Type 10 closes only when there are no verified inferred candidates, or the sole verified candidate is the person's current UKRDC master. This change does not introduce automatic relinking. Tests retain items while a required link is missing.

## 3. Merge Test Expectations

Accepted. Keeping types 4 and 8 separate is intentional: they describe different conditions. Both merge tests now expect three work items and assert the exact types 3, 4 and 8. The prior expectation of two relied on the old type-blind deduplication defect.

## 4. Person Object Identity And Source MRN

Accepted as an API weakness, with a qualification: the checked-in normal Repository path keeps the same Person, but PID migration reconstructs it after restoring national identifiers. Simply copying staged closures would be unsafe because migration temporarily removes those identifiers.

Transient closure IDs have been removed. Storage now finds OPEN type 9 items through persisted PIDXREF mappings using the final consolidated PID, original MRN and matching facility/extract attributes. It reevaluates the final incoming identifiers before changing database demographics, then applies closures in the storage transaction. Tests cover reconstructed requests, restored conflicting identifiers and failure rollback.

Both type 9 creation paths now write the MRN using `getUnconsolidatedLocalId()`, matching lookup. New PIDXREF allocation uses that same source ID. Missing or inconsistent historical source metadata is not guessed.

## 5. Validation Error Message Compatibility

No executable dependency on the old validation-message text was found in the checked-in Mirth channels. Core Repository and Internal-Group use response status; downstream consumers use status or an OK prefix. The new work-item-type wording was explicitly requested and is retained. The scenario documentation has been corrected.

External deployed channels, alert rules and monitoring configuration are not available here. Their compatibility cannot be established from this workspace and still requires a deployment check.

## 6. Type 9 Lookup Cost

Accepted. Source lookups now filter OPEN type 9 rows in SQL, excluding closed history and WIP items before returning candidates. They run during storage rather than on every successful Get/Set PID call. The durable lookup also requires an exact PIDXREF source mapping.

Fresh DDL includes a partial source-expression index for OPEN type 9 items. `Database/SCRIPTS/1.4.6.SQL` provides its optional concurrent creation for existing databases; it must run outside a transaction. Production query plans and timings have not been measured locally.
