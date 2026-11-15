# Database DDL and Scripts

## Reset database

* Open up a terminal session from Jtrace/Database/DDL in Eclipse

* Login into postgres 

`psql -U postgres`

Enter the password (`postgres`)

* Rebuild the db and schema

`\i JTRACE.DDL`

* Rebuild all the tables

`\i ../SCRIPTS/REBUILD_ALL_TABLES.DDL`

The fresh DDL already includes all migrations. Do not run historical upgrade scripts after a fresh rebuild.

## Upgrade Order

Run only the migrations not already applied to the database. The historical `1.6.10.SQL` filename refers to the **Data-Repository version**, not the JTRACE version.

| Order | Script | JTRACE Change |
| --- | --- | --- |
| 1 | `SCRIPTS/1.6.10.SQL` | JTRACE 1.3.8 (2019): timestamps, defaults and PIDXREF uniqueness. For pre-1.3.8 databases only. |
| 2 | `SCRIPTS/1.4.2.SQL` | Identifier values at least 50 characters; updated-by fields at least 320. |
| 3 | `SCRIPTS/1.4.3.SQL` | National identifier values and audit identifier values at least 50. |
| 4 | `SCRIPTS/1.4.5.SQL` | Convert fixed-width CHAR fields to VARCHAR and remove trailing padding. |
| 5 | `SCRIPTS/1.4.6.SQL` | Optional index for OPEN type 9 work-item source lookup. |
| 6 | `SCRIPTS/1.4.7.SQL` | National identifier organization/type fields at least 50, including support for `UKRR_UID`. |

Do not sort all filenames numerically: that would incorrectly put the 2019 migration last. A database running JTRACE 1.4.0 should already have the 1.3.8 changes.

The length migrations treat their sizes as minimums. They retain wider deployed columns, and do not impose a bound on existing unlimited VARCHAR/TEXT columns. The CHAR conversion trims trailing padding while converting, rather than updating CHAR values only to have PostgreSQL pad them again. Meaningful identifier content is not truncated.

Fresh-install DDL uses these minimum sizes. A deployed database with deliberate larger sizes may therefore remain wider than the baseline DDL. `person.localid` and `pidxref.localid` are 50; `tracerequest.localid` is a separate field whose baseline remains 10. PID and UKRDCID value widths are unchanged by this correction.

## Existing Deployments

Editing an old script does not update a database that has already run it. Apply `SCRIPTS/1.4.7.SQL` to widen `masterrecord.nationalidtype`, `person.nationalidtype` and `audit.mainnationalidtype` to at least 50. It can also be rerun safely without reducing larger definitions.

For example, from the repository root:

```powershell
psql -U postgres -d JTRACE -v ON_ERROR_STOP=1 -f "Database/SCRIPTS/1.4.7.SQL"
```

Back up the database and use a maintenance window: ALTER TABLE takes locks and CHAR-to-VARCHAR conversions may rewrite data/indexes. A trimming collision with a unique key must be investigated; the migration does not merge records or remove non-space characters to force it through. It cannot recover data already lost to an older/manual narrowing operation.

Run `1.4.6.SQL` separately, **outside a transaction**, because it creates an index concurrently. The historical `1.6.10.SQL` is not rerunnable on an upgraded schema; do not apply it just because its filename appears larger.

## Width Regression Test

`TESTS/migration_widths.SQL` checks baseline widths, `UKRR_UID` values, wider pre-existing columns and unlimited columns across repeated length migrations. It requires a dedicated database named `jtrace_migration_test`, replaces that database's test schema inside a transaction, and rolls back when successful:

```powershell
psql -U postgres -d jtrace_migration_test -v ON_ERROR_STOP=1 -f "Database/TESTS/migration_widths.SQL"
```
