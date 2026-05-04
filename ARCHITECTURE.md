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
- `IMAP` as a permanently supported mailbox-access communication protocol and compatibility layer, with the first major development push completed through `0.1.2`
- `ActiveSync` as the first mobile/native compatibility target for clients that support `Exchange ActiveSync`
- `EWS` as the active `0.1.3` Exchange compatibility implementation, without moving `SMTP` or canonical mailbox state out of `LPE`
- `MAPI over HTTP` as a guarded implementation track for future Outlook desktop support, with public edge routing and autodiscover publication only enabled deliberately for interoperability testing while the service matures
- `LPE-CT` as the distinct DMZ sorting center for external exposure, inbound `SMTP`, outbound relay, quarantine, and perimeter enforcement
- `LPE` as the system of record for mailboxes, contacts, calendars, tasks, rights, and user-visible state
- future local AI supported without requiring data to leave the server

## Current Delivery Priority

`IMAP` was the development-start compatibility layer through `0.1.2` and remains a supported communication protocol for mailbox access.
The current `0.1.3` product priority is implementing the selected `EWS` adapter
for Exchange-style clients while preserving the canonical mailbox, contacts,
calendar, and task model.

That means:

- `JMAP` first: complete state or change semantics, WebSocket reliability, and shared-mailbox behavior
- `IMAP` remains a supported client communication protocol and should receive correctness fixes for sync, `UID` behavior, flags, and real-client compatibility, but it is no longer the main `0.1.3` release driver
- `ActiveSync` as the flagship mobile/native-client story for clients that support `Exchange ActiveSync`: prioritize Outlook mobile and iOS compatibility labs, long-poll stability, send-flow correctness, and folder-sync edge cases
- `EWS` is the `0.1.3` Exchange compatibility focus for Exchange-style folder, mail, contacts, calendar, and task synchronization; it must not imply `RPC`, client `SMTP`, or a parallel `Sent` / `Outbox` model
- `MAPI over HTTP` work can proceed behind authenticated endpoints; `mapiHttp` autodiscover publication is opt-in for interoperability testing, and legacy `EXCH` / `EXPR` autodiscover publication requires its own explicit test switch so Outlook desktop `IMAP` setup is not hijacked before EMSMDB, NSPI, session context, and canonical mailbox synchronization are implemented enough for real Outlook desktop login
- Outlook for Windows desktop can continue to use the supported `IMAP` communication path when configured that way; `EWS` publication remains an explicit administrator choice until its limits are acceptable for the deployment
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

- `docs/architecture/jmap-mail-mvp.md`
- `docs/architecture/jmap-contacts-calendars-mvp.md`
- `docs/architecture/activesync-mvp.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/imap-mvp.md`
- `docs/architecture/sieve-managesieve-mvp.md`
- `docs/architecture/dav-mvp.md`
- `docs/architecture/tasks-mvp.md`

### Data, lifecycle, and compliance

- `docs/architecture/attachments-v1.md`
- `docs/architecture/collaboration-acl-delegation-mvp.md`
- `docs/architecture/data-lifecycle-and-compliance.md`

## Deferred Topics

The following topics are still intentionally deferred:

- administrative action journaling
