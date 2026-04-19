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
- `IMAP` as a compatibility mailbox-access layer
- `ActiveSync` as the first native Outlook/mobile compatibility target
- `EWS` kept as a future extension
- `LPE-CT` as the distinct DMZ sorting center for external exposure, inbound `SMTP`, outbound relay, quarantine, and perimeter enforcement
- `LPE` as the system of record for mailboxes, contacts, calendars, tasks, rights, and user-visible state
- future local AI supported without requiring data to leave the server

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
- `docs/architecture/web-design.md`
- `docs/architecture/local-llm.md`

### Edge, transport, and security

- `docs/architecture/edge-and-protocol-exposure.md`
- `docs/architecture/high-availability.md`
- `docs/architecture/lpe-ct-integration.md`
- `docs/architecture/mail-security-and-traceability.md`
- `docs/architecture/observability.md`
- `docs/architecture/client-autoconfiguration.md`

### Protocols

- `docs/architecture/jmap-mail-mvp.md`
- `docs/architecture/jmap-contacts-calendars-mvp.md`
- `docs/architecture/activesync-mvp.md`
- `docs/architecture/imap-mvp.md`
- `docs/architecture/sieve-managesieve-mvp.md`
- `docs/architecture/dav-mvp.md`
- `docs/architecture/tasks-mvp.md`

### Data, lifecycle, and compliance

- `docs/architecture/attachments-v1.md`
- `docs/architecture/data-lifecycle-and-compliance.md`

## Deferred Topics

The following topics are still intentionally deferred:

- administrative action journaling
