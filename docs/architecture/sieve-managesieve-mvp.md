# Sieve and ManageSieve

## Current State/Functionality Overview

`ManageSieve` manages per-account scripts stored in `LPE`; mailbox `Sieve` execution runs during final inbound delivery. Filtering must not move perimeter policy into core `LPE` or create a parallel routing engine.

## Implementation/Usage

- Canonical storage:
  - scripts are stored per mailbox account
  - active script state is account-scoped
- ManageSieve supports:
  - `AUTHENTICATE PLAIN`
  - `AUTHENTICATE XOAUTH2`
  - non-synchronizing literals `{N+}`
  - script upload, list, activate, retrieve, and delete operations
- Execution supports:
  - `fileinto`
  - `discard`
  - `redirect`
  - `vacation`
- Submission behavior:
  - `redirect` uses canonical `LPE` submission and outbound relay through `LPE-CT`
  - `vacation` uses canonical `LPE` submission and outbound relay through `LPE-CT`
  - no Sieve-specific transport engine
- Constraints:
  - no edge spam/filtering policy in core mailbox Sieve
  - no byte-identical replay guarantee for `redirect`
  - no dedicated web administration UI for scripts

## Outlook Rule Projection

Mailbox rules are canonical Sieve scripts. `GET /api/mail/rules`, private JMAP
`Rule/get`, `Rule/query`, `Rule/changes`, and `Rule/queryChanges`, and MAPI over
HTTP `RopGetRulesTable` all read from `sieve_scripts` and the canonical rule
change log. They do not create protocol-local rule rows, hidden Exchange rule
messages, or deferred-action-message stores.

The Outlook projection is intentionally bounded. It exposes the script id, name,
active state, source kind, Sieve-derived condition/action summaries, size,
updated timestamp, and an explicit unsupported Exchange feature list. Full
Exchange rule condition blobs, action blobs, provider-specific predicates,
client-only rule execution, delegate rule templates, and deferred-action
messages are not represented. Rule creation, update, activation, rename, and
delete remain the existing Sieve operations.

## Reference Table/List

| Command / action | Status |
| --- | --- |
| `AUTHENTICATE PLAIN` | supported |
| `AUTHENTICATE XOAUTH2` | supported |
| `{N+}` literals | supported |
| `fileinto` | supported |
| `discard` | supported |
| `redirect` | canonical resubmission |
| `vacation` | canonical resubmission |
| Outlook rule API/JMAP/MAPI read projection | supported from canonical `sieve_scripts` |
| Exchange rule blobs / deferred actions | unsupported |
