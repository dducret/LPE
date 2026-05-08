# IMAP Adapter

## Current State/Functionality Overview

`lpe-imap` exposes IMAP as a mailbox compatibility layer over canonical `LPE` mailbox state. It supports mailbox access and mutation without adding IMAP-specific mailbox, `Sent`, `Drafts`, or `Outbox` state.

## Implementation/Usage

- Authentication:
  - mailbox account credentials
  - supported mailbox auth tokens where configured
- Supported commands:
  - `LOGIN`
  - `CAPABILITY`
  - `NOOP`
  - `LOGOUT`
  - `LIST`
  - `XLIST`
  - `STATUS`
  - `CREATE`
  - `DELETE`
  - `RENAME`
  - `SELECT`
  - `EXAMINE`
  - `CHECK`
  - `CLOSE`
  - `UNSELECT`
  - `EXPUNGE`
  - `FETCH`
  - `UID FETCH`
  - `STORE`
  - `UID STORE`
  - `SEARCH`
  - `UID SEARCH`
  - `COPY`
  - `UID COPY`
  - `APPEND`
- Canonical behavior:
  - folder aliases such as `Deleted Items` and `Trash` converge on canonical trash
  - `APPEND` uses canonical draft or import persistence
  - flags update canonical message state
  - search uses canonical indexed fields and must not expose `Bcc`
  - UID behavior is scoped to mailbox state
- File validation:
  - any imported client-provided file must pass the canonical validation path
- Diagnostics:
  - expose enough session, command, and mailbox identifiers for traceability
  - avoid logging protected metadata or secrets

## Reference Table/List

| Area | Rule |
| --- | --- |
| Role | compatibility mailbox access |
| Storage | canonical `LPE` mailbox tables |
| `Sent` | canonical only |
| `Drafts` | canonical only |
| `Outbox` | no protocol-local implementation |
| Search | no `Bcc` exposure |
| External `SMTP` | `LPE-CT`, not IMAP adapter |
