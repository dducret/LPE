# LPE Architecture

This document intentionally stays short.

It defines the architectural base of `LPE` and points to the specialized documents that contain the detailed decisions.

## Core Summary

`LPE` is a modern mail and collaboration platform.

Its stable architectural base is:

- backend services written primarily in `Rust`
- project source code under `Apache-2.0`
- `PostgreSQL` as the main persistent store
- `JMAP` as the primary modern protocol axis
- `IMAP` as a permanently supported mailbox-access communication protocol and compatibility layer
- `ActiveSync` as the first mobile/native compatibility target for clients that support `Exchange ActiveSync`
- `EWS` as the active `0.2.0` Exchange compatibility implementation, without moving `SMTP` or canonical mailbox state out of `LPE`
- `MAPI over HTTP` as the primary `0.2.0` implementation track for classic Outlook for Windows Exchange-account support, with public edge routing present and autodiscover publication gated until the Outlook interoperability matrix passes; Outlook Anywhere / RPC over HTTP is a legacy compatibility shim for later `EXPR` publication, not the first implementation path
- full Outlook support as an explicit `0.2.0` release goal: Outlook mobile through `ActiveSync`, Exchange-style mail, contacts, calendar, and task compatibility through `EWS`, and classic Outlook for Windows Exchange-account support through `MAPI over HTTP`
- `LPE-CT` as the distinct DMZ sorting center for external exposure, inbound `SMTP`, outbound relay, quarantine, and perimeter enforcement
- `LPE` as the system of record for mailboxes, contacts, calendars, tasks, rights, and user-visible state
- future local AI supported without requiring data to leave the server

## Current Delivery Priority

`IMAP` remains a supported communication protocol for mailbox access.
The current `0.2.0` product priority is implementing the selected `EWS` adapter
and full classic Outlook `MAPI over HTTP` support while preserving the canonical
mailbox, contacts, calendar, and task model.

That means:

- `JMAP` first: complete state or change semantics, WebSocket reliability, and shared-mailbox behavior
- `IMAP` remains a supported client communication protocol and should receive correctness fixes for sync, `UID` behavior, flags, and real-client compatibility, but it is no longer the main `0.2.0` release driver
- `ActiveSync` as the flagship mobile/native-client story for clients that support `Exchange ActiveSync`: prioritize Outlook mobile and iOS compatibility labs, long-poll stability, send-flow correctness, and folder-sync edge cases
- `EWS` is the `0.2.0` Exchange compatibility focus for Exchange-style folder, mail, contacts, calendar, and task synchronization; it must not imply `RPC`, client `SMTP`, or a parallel `Sent` / `Outbox` model
- `MAPI over HTTP` must be completed first in `0.2.0` for Outlook 2016 and Outlook 2019 desktop: profile creation, EMSMDB mailbox synchronization, NSPI address book behavior, send and draft flows through canonical submission, reconnect behavior, cached-mode ICS stability, and authoritative `Sent` visibility
- Outlook Anywhere / RPC over HTTP is a legacy compatibility shim after the MAPI over HTTP path; when administrators publish legacy `EXPR` autodiscover metadata, `/rpc/rpcproxy.dll` must implement authenticated RPC/HTTP mailbox transport, not only HTTP authentication
- MAPI readiness evidence must distinguish project-owned local harness passes, Microsoft Remote Connectivity Analyzer passes, and real Outlook 2016 / 2019 cached-mode profile passes; public MAPI autodiscover waits for all three evidence classes
- Outlook for Windows desktop can continue to use the supported `IMAP` communication path when configured that way; administrators can explicitly publish `EWS` plus legacy `EXCH` / `EXPR` autodiscover metadata only behind the documented gates, while the first supported Exchange-account publication path is completed `MAPI over HTTP`
- `DAV` and `ManageSieve` after that: focus on correctness, canonical execution, and client-matrix interoperability rather than feature sprawl

Any proposal to add protocol breadth must be weighed against unfinished interoperability, sync, and canonical-state work in these existing adapters.

## High-Level Topology

The architecture is split into two responsibility zones:

- `LPE`, the core mailbox and collaboration platform
- `LPE-CT`, the DMZ sorting center and edge publication layer

The important non-negotiable rules are:

- `LPE-CT` is the unique external exposure point
- `LPE` should not be directly reachable from the public Internet
- all client-facing protocol adapters must converge on the canonical `LPE` mailbox model
- `Sent`, drafts, and outbound state must not be reimplemented in parallel by compatibility layers

## Documentation Index

### Foundation

- `docs/architecture/initial-architecture.md`
- `docs/architecture/tenancy-identity-and-administration.md`
- `docs/architecture/admin-federated-auth-mvp.md`
- `docs/architecture/mailbox-modern-auth-mvp.md`
- `docs/architecture/web-design.md`
- `docs/architecture/local-llm.md`

### Edge, transport, and security

- `docs/architecture/edge-and-protocol-exposure.md`
- `docs/architecture/high-availability.md`
- `docs/architecture/lpe-ct-integration.md`
- `docs/architecture/lpe-ct-local-data-stores.md`
- `docs/architecture/mail-security-and-traceability.md`
- `docs/architecture/observability.md`
- `docs/architecture/operations-and-disaster-recovery.md`
- `docs/architecture/client-autoconfiguration.md`

### Protocols

- `docs/architecture/0.2.0-protocol-depth-gates.md`
- `docs/architecture/imap-jmap-implementation-matrix.md`
- `docs/architecture/internationalized-mailbox-names.md`
- `docs/architecture/jmap-mail-mvp.md`
- `docs/architecture/jmap-contacts-calendars-mvp.md`
- `docs/architecture/notes-journal-reminders.md`
- `docs/architecture/activesync-mvp.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/mapi-full-object-support-execution.md`
- `docs/architecture/nspi-support-matrix.md`
- `docs/architecture/ews-interoperability-matrix.md`
- `docs/architecture/imap-mvp.md`
- `docs/architecture/sieve-managesieve-mvp.md`
- `docs/architecture/dav-mvp.md`
- `docs/architecture/tasks-mvp.md`

### Data, lifecycle, and compliance

- `docs/architecture/sql-schema-v2.md`
- `docs/architecture/attachments-v1.md`
- `docs/architecture/mailbox-storage-pools-roadmap.md`
- `docs/architecture/collaboration-acl-delegation-mvp.md`
- `docs/architecture/data-lifecycle-and-compliance.md`

## Deferred Topics

The following topics are still intentionally deferred:

- administrative action journaling
