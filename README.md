# LPE

`LPE` is a modern mail and collaboration platform written primarily in Rust.

The repository is aligned for release `0.5.2`.

`0.5.2` requires a new, empty SQL database initialized from the canonical
`0.5.2-sql` schema. It deliberately has no in-place schema upgrade path;
existing databases must be replaced only through an intentional fresh setup.

See `docs/releases/0.5.2.md` for the short release note.

## Initial Principles

- project code is licensed under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `PostgreSQL` is the primary metadata store
- `JMAP` is the main modern protocol axis
- `IMAP` is a permanently supported mailbox-access communication protocol and compatibility layer
- inbound and outbound `SMTP` transport is handled by the `LPE-CT` sorting center
- `ActiveSync` is the first targeted native mobile compatibility layer for clients that support `Exchange ActiveSync`
- `EWS` is the bounded Exchange compatibility implementation for Exchange-style mail, contacts, calendar, and task clients
- full Outlook support remains a release goal: Outlook mobile through `ActiveSync`, Exchange-style compatibility through `EWS`, and classic Outlook for Windows Exchange-account support through the `MAPI over HTTP` track
- the architecture remains compatible with future local AI without data leaving the server

## Current Priority

The current repository priority is implementing `EWS` and full classic Outlook `MAPI over HTTP` support while preserving the canonical `LPE` mailbox, contacts, calendar, tasks, and submission model.

The near-term order is:

- `JMAP` depth first: state or change semantics, WebSocket reliability, delegation, and shared-collection consistency
- `IMAP` support as a continuing client communication protocol: sync behavior, `UID` handling, flags, and client compatibility
- `EWS`: Exchange-style folder, mail, contacts, calendar, and task synchronization without `RPC`, client `SMTP`, or a parallel `Sent` / `Outbox`
- `MAPI over HTTP`: maintain the published classic Outlook for Windows Exchange-account path over authenticated `/mapi/emsmdb` and `/mapi/nspi`, with profile creation, mailbox sync, NSPI, send, reconnect, and canonical `Sent` behavior protected by regression tests and real-client runs
- `ActiveSync` as the flagship compatibility target for Outlook mobile and other native mobile clients
- `DAV` and `ManageSieve` interoperability work after the higher-priority mail protocols are stable

## Structure

- `crates/` Rust services and libraries, with `lpe-storage` also centralizing shared mail parsing used by imports and protocol adapters
- `web/admin` React/TypeScript back office
- `web/client` Outlook Web style client
- `LPE-CT/` separate DMZ sorting center with its own architecture and operations documentation
- `docs/architecture/` technical decisions and subsystem scope
- `docs/releases/` release notes
- `installation/` deployment scripts and documentation
- `LICENSE.md` project license text, accepted exceptions, and dependency policy

## Current Implemented Scope

The current repository already contains:

- a persistent administration console backed by `PostgreSQL`
- a canonical message submission flow with protected `Bcc` handling and outbound queueing toward `LPE-CT`
- an explicit internal `LPE` / `LPE-CT` HTTP integration contract
- MVP protocol adapters for `JMAP Mail`, `JMAP Contacts`, `JMAP Calendars`, `IMAP`, `ActiveSync`, `EWS`, `Sieve` / `ManageSieve`, `CardDAV`, and `CalDAV`
- canonical personal tasks exposed through the account workspace model
- public client autoconfiguration for `Thunderbird`, `Outlook`, `ActiveSync`, opt-in `EWS`, and enabled-by-default `MAPI over HTTP` on new 0.5.2 installations
- a web client backed by persistent account authentication and mailbox/workspace APIs
- first observability foundations with metrics and structured tracing

## Getting Started

The current workspace compiles with:

```powershell
cargo check
```

For installation and reset workflows on `Debian Trixie`, see [installation/README.md](installation/README.md).

### Focused `lpe-exchange` test areas

Named Cargo aliases let protocol work run the smallest relevant
`lpe-exchange` area instead of the complete suite after every change. The areas
overlap intentionally; run the full gate before a release or interoperability
deployment.

```text
# Exact regressions and contracts
cargo test-lpe-exchange-probe-g
cargo test-lpe-exchange-object-contracts

# Complete MAPI over HTTP request-level suite
cargo test-lpe-exchange-mapi-http

# MAPI over HTTP request-level subareas
cargo test-lpe-exchange-mapi-calendar
cargo test-lpe-exchange-mapi-connect
cargo test-lpe-exchange-mapi-contacts
cargo test-lpe-exchange-mapi-free-busy
cargo test-lpe-exchange-mapi-hierarchy
cargo test-lpe-exchange-mapi-local-replica-ids
cargo test-lpe-exchange-mapi-logon-profile
cargo test-lpe-exchange-mapi-notifications
cargo test-lpe-exchange-mapi-nspi
cargo test-lpe-exchange-mapi-permissions
cargo test-lpe-exchange-mapi-properties
cargo test-lpe-exchange-mapi-public-folders
cargo test-lpe-exchange-mapi-recoverable-items
cargo test-lpe-exchange-mapi-reminders
cargo test-lpe-exchange-mapi-rules
cargo test-lpe-exchange-mapi-save-changes
cargo test-lpe-exchange-mapi-submission
cargo test-lpe-exchange-mapi-sync
cargo test-lpe-exchange-mapi-tables
cargo test-lpe-exchange-mapi-tasks
cargo test-lpe-exchange-mapi-transport
cargo test-lpe-exchange-mapi-wlink-properties

# Broader implementation and protocol areas
cargo test-lpe-exchange-mapi-core
cargo test-lpe-exchange-calendar
cargo test-lpe-exchange-sync
cargo test-lpe-exchange-transport
cargo test-lpe-exchange-ews
cargo test-lpe-exchange-nspi
cargo test-lpe-exchange-rpc

# Complete lpe-exchange gate
cargo test-lpe-exchange
```

All aliases run serially to keep session, handle-map, and shared-store tests
deterministic. The MAPI Calendar alias also includes `calendar_identity_scope`,
and the MAPI sync alias also includes `sync_import_deletes`; those prefix
overlaps are intentional. The broader area aliases overlap the request-level
subareas and implementation-unit tests by name. They reduce the tests executed;
because the crate still uses one Rust unit-test binary, a source change can
still require that binary to be relinked. A physical binary split is a separate
test-support refactor. The broad EWS alias targets the `ews` module; the one
crate-root `ews_types` enum test remains in the complete gate. Autodiscover publication tests belong to
`lpe-admin-api`, not `lpe-exchange`.

## Repository Checks

Run the lightweight repository maintenance checks before review:

```sh
python3 tools/check_repository.py
```

Report tracked production source files above the 1,500-line threshold:

```sh
python3 tools/check_oversized_sources.py
```

The check is intentionally lightweight and portable for Debian Trixie. It scans
tracked source files and excludes lockfiles, generated/cache areas, `.git`,
`node_modules`, `target`, build output, and test directories by default.

Use `--fail` when the check should exit non-zero for CI enforcement:

```sh
python3 tools/check_oversized_sources.py --fail
```

The repository wrapper exposes the same enforcement mode as
`--fail-oversized`:

```sh
python3 tools/check_repository.py --fail-oversized
```

Use `--include-tests` when reviewing oversized test files as part of a planned
test-module split:

```sh
python3 tools/check_oversized_sources.py --include-tests
```
