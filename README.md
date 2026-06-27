# LPE

`LPE` is a modern mail and collaboration platform written primarily in Rust.

The repository is aligned for release `0.4.0`.

Release `0.4.0` requires a fresh empty SQL database initialized from the canonical schema.

See `docs/releases/0.4.0.md` for the short release note.

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
- `MAPI over HTTP`: complete the classic Outlook for Windows Exchange-account path over authenticated `/mapi/emsmdb` and `/mapi/nspi`; supported publication remains gated until profile creation, mailbox sync, NSPI, send, reconnect, and canonical `Sent` behavior pass the Outlook interoperability matrix
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
- public client autoconfiguration for `Thunderbird`, `Outlook`, `ActiveSync`, opt-in `EWS`, and guarded opt-in `MAPI over HTTP` interoperability testing
- a web client backed by persistent account authentication and mailbox/workspace APIs
- first observability foundations with metrics and structured tracing

## Getting Started

The current workspace compiles with:

```powershell
cargo check
```

For installation and reset workflows on `Debian Trixie`, see [installation/README.md](installation/README.md).

## Repository Checks

Report production source files above the 1,500-line threshold:

```sh
python3 tools/check_oversized_sources.py
```

Use `--fail` when the check should exit non-zero for CI enforcement:

```sh
python3 tools/check_oversized_sources.py --fail
```
