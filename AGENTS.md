# LPE Development Agent Context

This file defines the minimum context any AI agent must use when changing `LPE`.

## Reading Scope

Read only the documentation needed for the task.

Always read:

1. `ARCHITECTURE.md`
2. `docs/architecture/initial-architecture.md`
3. `LICENSE.md`

Read additional documents only when they are directly relevant to the change:

- `README.md` for repository overview or release framing
- `installation/README.md` for install, update, packaging, or operational work
- `docs/architecture/web-design.md` for any UI, layout, navigation, `Tailwind`, shared component, drawer, dialog, or responsive work
- `docs/architecture/attachments-v1.md` for attachment ingestion or indexing work
- `docs/architecture/local-llm.md` for AI-related work
- the specialized architecture document that matches the protocol or subsystem being changed

Do not read unrelated documentation "just in case". Minimize the amount of context sent to agents.

## Stalwart Reference

Stalwart is a product and architecture benchmark only.

Agents must not copy Stalwart code, and must not treat its architecture as directly reusable in `LPE`.

Mandatory constraints:

- all `LPE` source code must remain under `Apache-2.0`
- `MIT` dependencies are allowed only when no reasonable `Apache-2.0` alternative exists
- `AGPL`, `LGPL`, `GPL`, `SSPL`, and non-standard licenses are forbidden
- every new dependency or external implementation idea must be checked against `LICENSE.md`

## Non-Negotiable Architecture

`LPE` has two distinct responsibility areas:

- the core `LPE` server for mailboxes, contacts, calendars, tasks, storage, search, rights, and user-visible state
- the `LPE-CT` sorting center in the `DMZ` for inbound and outbound `SMTP`, filtering, quarantine, relay, traceability, and perimeter security

The sorting center is shared across domains and has its own administrators.

The core `LPE` server is multi-tenant. Each tenant manages its domain and domain mailboxes. `LPE` has global administrators and tenant administrators.

## Protocol Rules

- `JMAP` is the primary modern protocol
- `IMAP` is a mailbox compatibility layer
- internet-facing `SMTP` must stay in `LPE-CT`, not move back into the core `LPE` server
- client autodiscovery and autoconfiguration must publish only endpoints that are truly implemented and exposed
- the internal `LPE -> LPE-CT` relay must never be advertised as a client `SMTP` submission endpoint unless a real authenticated client-submission service is explicitly deployed and documented

The sorting center is responsible for:

- SMTP ingress from the Internet
- outbound relay
- authenticated outbound handoff reception from `LPE`
- authenticated final delivery toward `LPE`
- `DKIM` signing
- `SPF` and `DMARC` related policies
- retries
- outbound queue
- bounce and `DSN`

The core `LPE` server remains responsible for the canonical sent-message copy in `Sent`.

## Outlook and Native Client Rules

Native Outlook and mobile support is a first-class requirement.

- `ActiveSync` is the first targeted native Outlook/mobile compatibility layer
- `EWS` stays a future extension
- `IMAP` + `SMTP` + autodiscover must not be treated as sufficient for Outlook adoption
- every client layer must use the canonical `LPE` submission and synchronization model
- no client layer may implement parallel `Sent` or `Outbox` logic

Any message sent from Outlook, iPhone Mail, or another native client must be recorded in `LPE` and visible in `Sent`.

## Data, Security, and AI Rules

- the primary store is `PostgreSQL`
- search uses `PostgreSQL` by default
- identical attachments are deduplicated per domain, but export must reconstruct messages with their blobs
- `Bcc` is protected metadata and must not be indexed in user search or exposed to user-facing AI pipelines
- future AI must remain compatible with local-only execution; no AI feature may assume data leaves the server
- every external or client-provided file must be validated with Google `Magika` before normal processing

v1 attachment text indexing is limited to:

- `PDF`
- `DOCX`
- `ODT`

Do not extend that scope without explicit documentation updates.

Web interfaces must support at least `en`, `fr`, `de`, `it`, and `es`, with English as the default UI language.

## Working Method

- verify the documentation context before modifying code
- do not contradict documented architecture choices without updating the documentation explicitly
- if a change affects behavior, prerequisites, installation, release framing, or architecture, update the relevant documentation in the same work
- if a new durable rule appears, update `AGENTS.md`
- prefer explicit architectural documentation over leaving structural assumptions implicit in code
- for frontend work, converge on the shared Tailwind-based design system instead of one-off utility sprawl
- for administration UI lists in `LPE` and `LPE-CT`, use the default management pattern: full-width list, primary `New` or `Create` action in the list header, and a right-side drawer for creation, details, and contextual actions

## Installation Scope

- the initial Linux deployment target is `Debian Trixie`
- installation scripts must first target deployment from the Git repository
- Windows Server support is deferred and must not be assumed in Linux scripts

## Consistency Rule

When code, documentation, and `AGENTS.md` diverge:

1. identify the divergence
2. choose the option most consistent with explicit user constraints
3. update code and documentation together
