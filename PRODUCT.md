# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Tenant administrators operating mail and collaboration services for their domains. They provision and govern tenant domains, mailboxes, identities, access, quotas, and client interoperability while maintaining reliable user-visible state.

## Product Purpose

LPE is a multi-tenant mail and collaboration platform for mailboxes, contacts, calendars, tasks, storage, search, rights, and user-visible state. It provides a modern core while preserving practical compatibility with native and Exchange-style clients. Success means that tenant administrators can operate a dependable domain mail service whose users can work through supported Outlook and mobile client paths without divergent mailbox or submission state.

## Positioning

LPE combines a canonical, modern mail and collaboration core with a first-class Outlook compatibility path: MAPI over HTTP for classic Outlook for Windows, EWS for Exchange-style interoperability, and ActiveSync for compatible mobile clients. Compatibility layers converge on the same canonical state and submission flow rather than duplicating Sent or Outbox behavior.

## Operating Context

LPE runs as a core service behind the separate LPE-CT DMZ sorting center. Tenant administrators manage domains and mailbox services; the sorting center owns Internet SMTP ingress, outbound relay, filtering, quarantine, traceability, and perimeter security. The core uses PostgreSQL for persistent state and exposes web administration and webmail alongside protocol adapters.

## Capabilities and Constraints

- Primary modern protocol: JMAP; IMAP remains a mailbox compatibility layer.
- Outlook compatibility is a principal product priority through MAPI over HTTP, EWS, and ActiveSync where clients support it.
- Core source is Apache-2.0. MIT dependencies are allowed only when no reasonable Apache-2.0 alternative exists and the documented dependency policy permits them.
- LPE-CT, not the core LPE service, owns Internet-facing SMTP and perimeter mail transport.
- The web UI supports English by default plus French, German, Italian, and Spanish.
- All external or client-provided files require Google Magika validation before normal processing.
- Bcc remains protected metadata and must not enter user search or user-facing AI pipelines.
- Future AI capabilities must remain compatible with local-only execution.

## Evidence on Hand

- Repository implementation and architecture documents for the 0.5.2 release.
- `web/admin` contains the React/TypeScript administration console.
- `web/client` contains the Outlook Web-style client.
- No product marketing assets, customer evidence, testimonials, or external benchmarks were identified; future work must not invent them.

## Product Principles

- Preserve one canonical source of mailbox and collaboration truth across every client and protocol adapter.
- Make Outlook compatibility real-client reliable, not merely endpoint-complete.
- Keep Internet-facing mail transport and perimeter enforcement isolated in LPE-CT.
- Give tenant administrators clear, trustworthy operational control over their domains and mail services.
- Preserve data control through local-first architecture and protected metadata handling.

## Accessibility & Inclusion

Web interfaces must support English, French, German, Italian, and Spanish, with English as the default UI language.
