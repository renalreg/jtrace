# JTRACE Work Item And Error Scenarios

This document describes the work items and explicit errors raised by the JTRACE code.

## Work Items

Work items are created by `UKRDCIndexManager` when a record can be stored or linked, but requires investigation or manual review. A work item is not necessarily a rejected record.

| Number | Type | Scenario | Code description |
| --- | --- | --- | --- |
| 1 | `TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY` | A person already linked to a UKRDC primary identifier has changed demographics. The incoming record is not newer than the current UKRDC master, and the person no longer verifies against that master. | `Stale Demographics Not Verified Against PrimaryId` |
| 2 | `TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY` | An incoming record claims a primary UKRDC identifier which already has a master record, but the incoming demographics do not verify against it. JTRACE still creates the link and marks the master for investigation. | `Claimed Link to Primary Id Not Verified` |
| 3 | `TYPE_INFERRED_LINK_NOT_VERIFIED_PRIMARY` | No primary identifier is supplied. A national identifier implies a link to an existing UKRDC master, but the demographics do not verify. | `Link to Inferred PrimaryId not verified` |
| 4 | `TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL` | An incoming national identifier already has a master record, but the incoming demographics do not verify against that national master. The link is created and the master is marked for investigation. | `Claimed Link to NationalId Not Verified` |
| 5 | `TYPE_STALE_DEMOGS_NOT_VERIFIED_NATIONAL` | An existing national link has changed demographics. The incoming record is stale, and the person no longer verifies against the national master. | `Stale Demographics Not Verified Against NationalId` |
| 6 | `TYPE_DEMOGS_NOT_VERIFIED_AFTER_PRIMARY_UPDATE` | A UKRDC master’s demographics are updated and another linked person no longer verifies against the updated primary master. | `Demographics Not Verified Following Update of Primary Id` |
| 7 | `TYPE_DEMOGS_NOT_VERIFIED_AFTER_NATIONAL_UPDATE` | A national master’s demographics are updated and another linked person no longer verifies against the updated national master. | `Demographics Not Verified Following Update of National Id` |
| 8 | `TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR` | More than one person from the same originator is linked to the same national master. | `More than 1 record from Unit:<originator> linked to master:<masterId>` |
| 9 | `TYPE_XREF_MATCHED_NOT_VERIFIED` | A changed local identifier is matched through sending facility, sending extract, and a national identifier, but the incoming demographics do not fully match the existing person. This is raised during both local-PID validation and update matching. | `Person matched by facility, extract and national id - not matched by demographics` |
| 10 | `TYPE_MULTIPLE_UKRDC_MATCH` | A national identifier produces more than one verified UKRDC candidate. The first verified candidate is used and additional candidates generate this work item. | `Additional verified UKRDC Link (see masterId) implied (see attributes)` |

### Matching Used For Work Items

The detailed verification rules are implemented in `UKRDCIndexManager.verifyMatch()`:

- An exact date-of-birth match verifies immediately.
- Otherwise at least two date-of-birth components must match.
- The first three surname characters and first given-name character must then match.
- The local PID/XREF matching path is stricter and requires matching gender, all date-of-birth components, surname, and given name.

Work-item creation is part of the current JTRACE transaction. If the surrounding API operation subsequently fails and rolls back, the work item is rolled back with it.

## Explicit Errors

### Person Validation

`UKRDCIndexManager.validateInternal()` raises these errors:

- `Invalid primary id type`: a supplied primary ID type is not `UKRDC`.
- `Surname must be at least 2 characters`.
- `Given Name must be at least 1 character`.
- `Gender must be at least 1 character`.
- `Date Of Birth is mandatory`.
- `LocalId must be at least 1 character`.
- `LocalId type must be present`.
- `Originator must be present`.

The national-ID processing path also raises:

- `NationalId not set`: a national identifier has a null ID or type.
- `Invalid Parameters`: invalid arguments were supplied to national-link updating.

### Local PID Matching Responses

These are returned as failed API responses rather than thrown `MpiException` messages:

- `FAILED TO MATCH DURING VALIDATION - SEE WORK ITEMS`: local PID matching found a national-ID/XREF candidate whose demographics did not verify.
- `FAILED TO MATCH DURING UPDATE - SEE WORK ITEMS`: the same condition occurred during the update/set-PID phase.

### Search, Link, And Unlink Errors

- `SearchRequest not provided`: a search request is null.
- `NationalId Must be provided`: a search request has no national identifier.
- `Invalid Date Of Birth Format`: the search date cannot be parsed as `yyyy-MM-dd`.
- `Incomplete parameters for link`: required manual-link parameters are missing.
- `Link already exists`: the requested person/master link already exists.
- `Link Record does not exists`: an unlink was requested for a link that is absent.
- `Incomplete parameters for unlink`: required unlink parameters are missing.
- `Master Record does not exists`: the requested master record is absent.
- `Person does not exists`: the requested person record is absent.
- `Linked Person Record does not exists`: a link references a person record that cannot be loaded.
- `Master ID does not exist`: a requested master ID cannot be loaded as a UKRDC ID.
- `Master ID is not a UKRDC ID`: the requested master exists but is not a UKRDC master.

### Work Item API Errors

The Work Item API raises these validation errors:

- `WorkItem Id must be provided`.
- `Status must be provided`.
- `Status provided is not valid`: status is greater than 3.
- `Description must be provided`.
- `Updated By user must be provided`.
- `Cannot update - work item does not exist`.
- `Person Id must be provided`.
- `Master Id must be provided`.

### Connection And Configuration Errors

- `Invalid database configuration. 1 or more null parameters`.
- `Invalid database configuration. 1 or more empty parameters`.
- `CONN POOL ERROR:<database error>`: the JTRACE connection pool could not be created or a connection could not be obtained.
- `Failure closing Connection<database error>`: closing the connection failed.

### Persistence Errors

JTRACE wraps database and generated-key failures in these specific messages:

- `Person read failed. <database error>`
- `Person create failed. <database error>`
- `Person update failed. <database error>`
- `Person delete failed. <database error>`
- `Person has no ID - cannot update.`
- `PidXREF insert failed`
- `PIDXREF delete failed`
- `Failure getting PidXREF. <database error>`
- `Failure querying PidXREF. <database error>`
- `MasterRecord insert failed. <database error>`
- `MasterRecord update failed. <database error>`
- `MasterRecord delete failed`
- `Failure getting MasterRecord. <database error>`
- `Failure querying MasterRecord. <database error>`
- `Failure querying UKRDC Sequence. <database error>`
- `LinkRecord insert failed`
- `LinkRecord delete failed`
- `Failure querying LinkRecord. <database error>`
- `Failure querying countByMasterAndOriginator. <database error>`
- `Failure querying countByMasterAndOriginatorExcludingPid. <database error>`
- `Audit insert failed`
- `Audit delete failed`
- `Failure querying Audit. <database error>`
- `WorkItem insert failed`
- `WorkItem delete failed`
- `Failure querying WorkItem. <database error>`
- `TraceRequest insert failed`
- `TraceRequest read failed`
- `TraceResponse insert failed`
- `TraceResponse read failed`
- `TraceResponseLine insert failed`
- `TraceResponseLine read failed`
- `trace delete failed`

The `<database error>` portion may contain the underlying PostgreSQL error, including constraint violations, missing relations, invalid schemas, or field-length errors.

### API Response Behaviour

The public API command wrappers catch exceptions, roll back the JTRACE transaction, close the connection, and return a failed response containing the exception message and stack trace. A successful JTRACE operation commits its transaction before returning.
