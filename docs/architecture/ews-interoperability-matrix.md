# EWS Interoperability Matrix

This matrix defines the repeatable `EWS` compatibility checks for the bounded
`0.1.3` Exchange adapter. It does not widen the adapter into a full Exchange
server and must stay aligned with `docs/architecture/ews-mapi-mvp.md`.

## Live Smoke Harness

`tools/ews_live_smoke_check.py` is the operator-run live smoke harness for a
deployed `EWS` endpoint. It uses only Python standard-library modules and reads
credentials from environment variables by default:

```powershell
$env:LPE_EWS_URL = "https://mail.example.test/EWS/Exchange.asmx"
$env:LPE_EWS_EMAIL = "user@example.test"
$env:LPE_EWS_PASSWORD = "<password>"
python tools/ews_live_smoke_check.py
```

The default checks are read-only:

- `GetServerTimeZones` returns the minimal supported time-zone catalog
- `FindFolder` exposes mailbox, contacts, calendar, and task folders
- `GetUserOofSettings` projects canonical `Sieve` vacation state
- `GetUserAvailability` returns authenticated-mailbox busy blocks

The optional `--mutating` mode creates and deletes a temporary task through
`CreateItem` and `DeleteItem`. Use it only against a mailbox intended for
interoperability testing.

## Required Release Checks

Run these checks before enabling `LPE_AUTOCONFIG_EWS_ENABLED` for a deployment:

- `EWS-01`: live smoke harness read-only mode against the public HTTPS endpoint
- `EWS-02`: live smoke harness `--mutating` mode against a disposable test mailbox
- `EWS-03`: day-two `SyncFolderItems` replay for mailbox, contacts, calendar, and tasks
- `EWS-04`: attachment get/create/delete with `Magika` validation and MIME export
- `EWS-05`: protected `Bcc` verification for normal mailbox reads versus `Drafts` / `Sent`
- `EWS-06`: OOF enable, read, disable through canonical `Sieve` vacation state
- `EWS-07`: simple calendar recurrence create/read/update/delete without recurrence expansion

## Deferred Checks

The following require canonical model or policy work before they can be
converted into passing interoperability checks:

- cross-mailbox free/busy and tenant address-book policy
- attendee response workflows and meeting update/cancel semantics
- reminders and alarms
- recurrence expansion, exceptions, and detached instances
- GAL-backed `ResolveNames`
