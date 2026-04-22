# LPE

`LPE` is a modern mail and collaboration platform written primarily in Rust.

The repository is aligned for release `0.1.3`.

Release `0.1.3` is a breaking change. Legacy updates are not supported. A fresh install and database recreation are required.

See `docs/releases/0.1.3.md` for the short release note.

## Initial Principles

- project code is licensed under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `PostgreSQL` is the primary metadata store
- `JMAP` is the main modern protocol axis
- `IMAP` is a mailbox compatibility layer
- inbound and outbound `SMTP` transport is handled by the `LPE-CT` sorting center
- `ActiveSync` is the first targeted native Outlook and mobile compatibility layer
- `EWS` remains a future extension after the canonical submission and synchronization model is stable
- the architecture remains compatible with future local AI without data leaving the server

## Current Priority

The current repository priority is to finish the coherence of the implemented protocol set before adding new protocols.

The near-term order is:

- `JMAP` depth first: state or change semantics, WebSocket reliability, delegation, and shared-collection consistency
- `IMAP` correctness next: sync behavior, `UID` handling, flags, and client compatibility
- `ActiveSync` as the flagship compatibility target for `Outlook` and mobile clients
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
- MVP protocol adapters for `JMAP Mail`, `JMAP Contacts`, `JMAP Calendars`, `IMAP`, `ActiveSync`, `Sieve` / `ManageSieve`, `CardDAV`, and `CalDAV`
- canonical personal tasks exposed through the account workspace model
- public client autoconfiguration for `Thunderbird` and minimal `Outlook` autodiscovery for `ActiveSync`
- a web client backed by persistent account authentication and mailbox/workspace APIs
- first observability foundations with metrics and structured tracing

## Getting Started

The current workspace compiles with:

```powershell
cargo check
```

For installation and reset workflows on `Debian Trixie`, see [installation/README.md](installation/README.md).
