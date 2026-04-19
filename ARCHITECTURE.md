# LPE Architecture

## Purpose

`LPE` is a modern mail and collaboration server.

It is responsible for hosting and operating the mailboxes of the domains under its control. Beyond email, the platform also manages contacts, calendars, and to-do lists.

The product architecture must remain consistent with the following core constraints:

- backend services are written primarily in `Rust`
- all project source code is licensed under `Apache-2.0`
- `PostgreSQL` is the primary persistent store
- `JMAP` is the primary modern protocol axis
- `IMAP` remains a mailbox-access compatibility layer
- inbound and outbound `SMTP` transport is handled by the sorting-center layer
- the architecture must remain compatible with future local AI capabilities without sending data outside the server perimeter

## Architectural Model

### Stalwart-inspired reference, with mandatory LPE divergences

Stalwart is a strong functional reference for `LPE`.

Its all-in-one mail and collaboration approach is aligned with the target product direction: a Rust-based server exposing modern mail and collaboration protocols, strong security, multi-tenancy, flexible storage, search, administration, and observability.

However, `LPE` must not be treated as a Stalwart clone or as a derivative implementation.

The following differences are mandatory:

- `LPE` source code must remain under `Apache-2.0`
- dependencies and reused code must comply with the project license policy
- Stalwart's `AGPL-3.0` and proprietary enterprise licensing model is not compatible with the `LPE` source-code policy
- `LPE` must keep a distinct DMZ sorting-center layer for inbound and outbound mail transport
- the mailbox and collaboration core must remain separated from the Internet-facing transport gateway
- native Outlook and mobile compatibility is adoption-critical, with `ActiveSync` as the first targeted compatibility layer and `EWS` kept as a future extension

Stalwart can therefore be used as a product and architecture benchmark, but not as a codebase to copy from unless each reused element is explicitly verified as license-compatible.

### Internal model first

`LPE` is not designed as an `IMAP` or `SMTP` server with extra features attached later.

The business core is centered on a stable internal mail and collaboration model. External protocols are adapters around that model:

- `JMAP` is the primary modern access layer for mail and collaboration data
- `IMAP` is a compatibility access layer for existing mail clients
- `SMTP` is a transport and interoperability concern owned by the sorting-center layer for exposed ingress and outbound relay

In addition to online protocols, `LPE` supports mailbox import and export in `PST` format for migration and interoperability scenarios.

### Protocol responsibility split

`LPE` does not need to implement every external protocol as an Internet-facing service.

The protocol split should follow the architectural boundary between the mailbox platform and the sorting-center transport layer:

- `LPE` should implement `JMAP` as the primary modern mailbox and collaboration protocol
- `LPE` should implement `IMAP` as a mailbox-access compatibility layer
- `LPE` should target `ActiveSync` as the first native Outlook and mobile compatibility layer
- `EWS` remains a future extension to evaluate after the canonical submission and synchronization model is stabilized
- `LPE` should expose collaboration compatibility through standards-based protocols when appropriate, including `CalDAV`, `CardDAV`, and potentially `WebDAV`
- Internet-facing `SMTP` ingress should be handled by the sorting-center layer, not by `LPE`
- outgoing `SMTP` relay responsibilities should also remain in the sorting-center layer

This keeps transport and edge-security logic concentrated in the sorting centers while keeping mailbox and collaboration logic concentrated in `LPE`.

### Client compatibility and sent-message consistency

Compatibility protocols such as `IMAP` and `ActiveSync` are important because users may rely on native client applications, including mobile clients such as the iPhone Mail application.

Native Outlook compatibility is especially important for adoption.

`LPE` should not assume that `IMAP` plus `SMTP` plus automatic account discovery is sufficient for Outlook users. The first targeted native Outlook and mobile compatibility layer is `ActiveSync` because it can cover mailbox, calendar, contact, synchronization, and mobile-client adoption needs through one coherent access model.

`EWS` remains a future extension. It should be evaluated after the canonical `LPE` submission and synchronization model is stabilized, not implemented as an early parallel model.

The architecture must therefore ensure that:

- users can connect supported clients to their `LPE` mailbox through an appropriate access protocol
- message submission performed through compatibility workflows is still reflected correctly in the mailbox state maintained by `LPE`
- sent messages are recorded in `LPE` so that the mailbox remains consistent across devices and protocols
- Outlook-compatible access is treated as a strategic adoption requirement, not as a later cosmetic add-on
- every client layer uses the canonical `LPE` submission and synchronization model
- no client compatibility layer writes its own parallel `Sent` or `Outbox` logic

This means protocol compatibility must not be implemented as a transport bypass that would cause sent messages to exist only in the client view or only in the sorting-center relay path.

All client-facing protocols must converge on the same canonical mailbox state, including `Sent`, draft, pending-send, and outbound-queue transitions.

### Autoconfiguration and autodiscovery

`LPE` should support client auto-configuration and autodiscovery workflows where they improve adoption and reduce setup errors.

This includes standards or de facto standards used by desktop and mobile clients to discover the correct access and submission endpoints.

Autodiscovery must describe the selected `LPE` access protocols and the sorting-center submission or relay path without weakening the architectural separation between mailbox access and SMTP transport.

### Physical mail analogy

The inbound mail architecture follows a model close in principle to historical physical mail handling.

A sender places a letter into an outgoing mailbox. Postal workers collect it, route it through one or more sorting centers, and finally deliver it into the recipient's mailbox.

`LPE` adopts the same operational logic for electronic mail:

- messages are received by one or more sorting centers
- sorting centers inspect, classify, and route messages
- accepted messages are delivered into the recipient mailbox hosted by `LPE`
- rejected or undeliverable messages remain traceable and can be quarantined

This analogy is deliberate because it clarifies operational responsibilities and system boundaries.

## High-Level Topology

### Multi-tenant model and isolation

`LPE` is a multi-tenant platform.

Each tenant is responsible for its own domain and the mailboxes attached to that domain.

The core isolation principle is:

- tenant data is isolated at domain scope
- tenant administration is scoped to the tenant domain
- only the DMZ sorting layer is shared across domains

This means the architecture must separate clearly between:

- tenant-owned mailbox and collaboration data
- tenant-scoped administrative capabilities
- shared transport-edge infrastructure

### Shared DMZ sorting infrastructure

Sorting centers in the `DMZ` are the main shared infrastructure component across domains.

They are mutualized across tenants, but they must still enforce domain-aware routing, filtering policy application, and tenant isolation in administrative and operational workflows.

Sorting centers have their own administrators, distinct from tenant administrators.

### Sorting centers in the DMZ

Inbound email first reaches one or more mail sorting centers deployed in the `DMZ`.

These sorting centers are responsible for edge mail intake and the first stage of message security and routing control. They are not the authoritative mailbox store themselves. Their role is to protect, classify, trace, and route traffic before final mailbox delivery.

These sorting centers may run as a cluster.

Clustered operation must support:

- horizontal scaling of inbound message handling
- shared or replicated mail trace history
- consistent filtering behavior across nodes
- resilient routing and quarantine handling when one node becomes unavailable

### Secure communications between sorting centers and the core platform

Communications between sorting centers, and between sorting centers and the core `LPE` platform, must be strongly secured.

This security requirement is not limited to transport encryption in transit. The architecture must also prevent successful man-in-the-middle interception or node impersonation inside the operational perimeter.

The architecture should therefore assume:

- mutually authenticated encrypted transport between trusted nodes
- explicit node identity validation
- machine or service identity with strong attestation
- rejection of unauthenticated or improperly authenticated peer nodes
- credential rotation procedures compatible with clustered operation

In practice, inter-node trust must be based on strong mutual authentication rather than on network location alone.

The current functional integration contract is documented in `docs/architecture/lpe-ct-integration.md`. In v1, that contract uses mutually trusted internal HTTP endpoints with a shared integration key, while keeping Internet-facing `SMTP` strictly on `LPE-CT`.

The preferred trust model is machine or service identity with strong attestation rather than a traditional PKI-heavy operational model.

This preference exists to avoid introducing unnecessary certificate-management infrastructure while still requiring a high-assurance identity model between trusted nodes.

The attestation model should be hybrid and proportional to operational criticality.

`LPE` should support a simpler trust configuration for small deployments, such as a single sorting center paired with a single `LPE` server operated by one administrator with basic operational skills, while also supporting stronger attestation models for larger or higher-risk environments.

At the architectural level, this means:

- lower-complexity deployments must be able to establish secure trusted-node communication without requiring heavy certificate-management infrastructure
- higher-criticality deployments should be able to use stronger machine or service attestation mechanisms
- the trust model should scale upward without requiring a redesign of the mail flow architecture

The security baseline must remain strong enough to prevent trivial node impersonation and man-in-the-middle attacks even in simplified deployments, but operational complexity should remain aligned with the size and skill profile of the target installation.

### Mailbox platform

The core `LPE` platform is responsible for mailbox ownership and user-facing collaboration data.

It manages:

- email mailboxes for hosted domains
- contacts
- calendars
- to-do lists

The mailbox platform remains the system of record for accepted user data, while DMZ sorting centers act as the controlled intake and routing layer for inbound mail flow.

`LPE` supports both:

- global administrators with server-wide and tenant-wide authority
- tenant administrators with authority limited to their own tenant scope

## Inbound Mail Flow

### Processing stages

The inbound mail pipeline is organized around the following stages:

1. message reception by a DMZ sorting center
2. initial technical validation of the SMTP session and message provenance
3. security and abuse filtering
4. message trace registration with a unique internal identifier
5. routing decision
6. final delivery to the destination mailbox platform or quarantine retention

### Initial validation

At the edge, sorting centers perform a first validation pass that may include:

- `DKIM` validation
- `SPF` validation
- `HELO` or `EHLO` consistency checks against the claimed sender domain
- other transport and provenance checks needed by policy

This stage is intended to reject clearly invalid or suspicious traffic as early as possible.

### Security and anti-abuse filtering

Sorting centers also perform malware and spam filtering before mailbox delivery.

The architecture currently anticipates controls such as:

- virus scanning
- spam scoring
- `Razor v2`
- `Pyzor`
- `RBL` lookups
- botnet-oriented reputation or detection checks

The exact implementation can evolve, but the architectural rule is stable: edge sorting centers are responsible for the first security and anti-abuse filtering pass before a message reaches a hosted mailbox.

### Mandatory incoming file-type validation with Magika

Every file entering the platform through an external connection or through a client-facing workflow must be validated with Google `Magika` before it is accepted into normal processing.

This rule applies to both:

- `LPE-CT`, for files received through external ingress paths
- `LPE`, for files submitted through client or API workflows

Typical covered paths include:

- email attachments received from external senders
- files attached or imported through webmail or future native-client submission flows
- `PST` import uploads
- `JMAP` blob upload paths
- any future browser, API, or protocol-level file upload entry point

Magika validation is an architecture-level content-type gate, not a UI hint.

The platform must therefore:

- classify incoming file bytes using `Magika`
- record the detected content type and confidence
- compare detected type with declared MIME type, file extension, and workflow expectations
- route the result into accept, restrict, quarantine, or reject policy decisions

`Magika` classification must happen before attachment indexing, import processing, archive ingestion, or downstream content-specific parsing.

### Injection-oriented content inspection

Sorting centers should also perform a first-pass inspection for dangerous or suspicious content patterns beyond traditional spam and malware categories.

This inspection scope should include potential:

- `SQL` injection payloads
- AI or prompt injection patterns
- script or command injection attempts
- other policy-defined content abuse patterns

The purpose of this inspection is not to reinterpret every message as an application request, but to identify payloads that are suspicious enough to influence quarantine, delivery policy, downstream trust, or operator review.

This inspection layer should produce a dedicated score or set of scores that can be combined with other filtering results.

### Domain-level policy control

The sensitivity and action thresholds for injection-oriented inspection should be configurable at domain level in the core `LPE` server policy plane.

This means the architecture should support:

- per-domain inspection policy
- per-domain threshold tuning
- policy-driven actions based on resulting scores
- propagation of effective policy from the core server to sorting centers

Possible policy-driven outcomes may include:

- accept
- tag
- quarantine
- defer
- reject

The architectural principle is that sorting centers execute the inspection close to the edge, while the authoritative policy and threshold configuration are managed centrally by `LPE` at domain scope.

The same policy plane should be able to govern how `Magika` validation mismatches and unsupported file-type outcomes are handled for each domain.

## Traceability and Message History

### Unique message processing identifier

Every message processed by a sorting center must receive a unique internal identifier.

This identifier is added to the processed message and is also stored in the message trace history. Its purpose is to provide operational traceability across filtering, routing, quarantine, delivery investigation, and audit workflows.

### Minimum retained metadata

Sorting centers must retain at least the following metadata for each processed message:

- processing date and time
- unique internal identifier
- sender IP address
- filtering score
- sender address
- recipient address
- subject
- delivery status
- mail flow direction
- message size
- whether the message is encrypted
- content inspection or injection-related score data when such inspection is enabled
- `Magika` file-type validation outcomes when file payloads are present

This history is required to support:

- auditability
- operational troubleshooting
- message trace search
- quarantine investigation
- delivery follow-up

## Quarantine and Delivery Outcomes

If a message is not delivered to its intended recipient, the sorting center must retain it in quarantine according to policy.

Quarantine is part of the normal architecture, not an optional side mechanism. It exists to preserve operator visibility and controlled handling of suspicious, blocked, or undeliverable messages.

Delivery outcome states should be modeled explicitly so that trace search and operations tooling can distinguish at minimum between:

- accepted and delivered
- accepted but quarantined
- rejected during filtering or validation
- temporarily deferred
- permanently undeliverable

## Responsibility Boundaries

### DMZ sorting layer

The DMZ sorting layer is responsible for:

- receiving inbound mail traffic
- performing first-pass sender and protocol validation
- performing first-pass virus and spam filtering
- assigning the unique processing identifier
- retaining mail trace metadata
- quarantining messages that are not delivered
- routing accepted messages toward the internal mailbox platform

### Core mailbox and collaboration layer

The core `LPE` platform is responsible for:

- mailbox persistence for hosted domains
- end-user access through `JMAP`
- compatibility access through `IMAP`
- canonical sent-message persistence for supported client submission workflows
- contacts, calendars, and to-do list management
- long-term business data consistency in `PostgreSQL`

The DMZ sorting layer is also responsible for outbound mail relay functions.

## Outbound Mail Flow

Outbound mail is routed through the sorting-center layer, which acts as the outgoing relay.

The sorting center is responsible for:

- outbound relay execution
- `DKIM` signing
- enforcement of `SPF` and `DMARC` related policy behavior on the sending side
- retry management
- outbound queue management
- bounce and `DSN` handling

This keeps mail transport responsibilities concentrated in the same controlled edge layer that already handles inbound security, traceability, and routing.

`LPE` remains responsible for the authoritative mailbox-side representation of sent mail.

The architectural principle is:

- `LPE` owns the mailbox state
- the sorting center owns SMTP transport execution

In the current functional implementation, this split is made explicit through two internal contracts:

- `LPE` hands outbound work to `LPE-CT` through an authenticated internal handoff API consumed by an outbound worker over `outbound_message_queue`
- `LPE-CT` performs final inbound delivery to the core mailbox platform through an authenticated internal delivery API instead of LAN-facing mailbox `SMTP`

This separation must still guarantee that user-submitted outbound messages are recorded in the appropriate sent-mail view inside `LPE`.

The current `JMAP Mail` MVP follows that rule explicitly:

- `Mailbox/get`, `Email/query`, and `Email/get` read from the canonical mailbox store
- `Email/set` persists only draft-state mailbox data
- `EmailSubmission/set` reuses the canonical submission workflow that writes `Sent` and `outbound_message_queue`
- no `JMAP` path performs direct Internet-facing `SMTP`
- protected `Bcc` metadata remains structurally separate from ordinary mailbox search and query projections

The current `ActiveSync` MVP follows the same rule:

- `Provision`, `FolderSync`, `Sync`, and `SendMail` are implemented as an adapter in `crates/lpe-activesync`
- account authentication is reused rather than duplicated
- `Sync` reads canonical mailbox, contact, and calendar data
- draft creation, modification, and deletion reuse the existing canonical draft workflow
- `SendMail` reuses canonical submission so the authoritative `Sent` copy exists before `LPE-CT` performs outbound relay
- no `ActiveSync` path performs direct Internet-facing `SMTP`
- the supported scope and limitations are documented in `docs/architecture/activesync-mvp.md`

The current `CardDAV` and `CalDAV` MVP follows the same rule:

- `CardDAV` and `CalDAV` are compatibility adapters over canonical collaboration data
- account authentication is reused rather than duplicated
- `contacts` and `calendar_events` remain the source of truth for the data exposed through DAV
- the first adapter supports minimal collection discovery, resource reads, and full-resource replacement
- the supported scope and limitations are documented in `docs/architecture/dav-mvp.md`

## Data and Storage Principles

- `PostgreSQL` is the primary persistent store for platform metadata and operational data
- message search and data models must prioritize performance, especially in `PostgreSQL`
- the architecture should preserve a clear distinction between edge intake responsibilities and authoritative mailbox data
- future local AI support must remain an internal capability and must not require external data export

## Encryption at Rest and Key Management

### Optional encryption at rest

Encryption at rest is optional and policy-driven.

It can be enabled for one or more of the following data classes:

- message bodies
- attachments
- search indexes
- archives
- sensitive metadata

The architecture should therefore allow selective activation rather than requiring an all-or-nothing model.

### Key model

The key-management principle follows an asymmetric public-key and private-key model comparable in principle to `SSL` and `TLS`.

The detailed implementation may evolve, but the architectural requirement is that protected data classes can rely on an asymmetric trust model for encryption-related operations.

### Performance implications

The architecture should explicitly recognize that encryption at rest and blob deduplication can affect performance characteristics.

This is especially relevant for:

- search indexing
- archive access
- very large mailboxes
- attachment reconstruction

Performance-sensitive deployments must therefore treat encryption and deduplication settings as architectural inputs, not as purely transparent toggles.

## Mailbox Storage and Growth Management

### Problem statement

Large mailbox growth is a first-class architectural concern.

Email retention and archiving requirements continue to increase, while many users do not actively clean up their mailboxes. As a result, mailbox sizes above `100 GB` must be treated as a normal scaling scenario rather than an operational exception.

The architecture must therefore provide a built-in storage placement and lifecycle strategy for:

- mailbox growth
- backup isolation
- migration simplicity
- large-mailbox performance control
- online archiving with degraded but usable access when required

### Database placement model

The storage model should distinguish between:

- a server-level database for shared platform and control-plane data
- mailbox data databases that can be shared by multiple mailboxes
- dedicated mailbox databases for large mailboxes

This separation is intended to make backup, restore, migration, and operational isolation easier than a single-database design for the entire platform.

At the architectural level, `LPE` should support database placement by server, by group of mailboxes, and by individual mailbox when needed.

### Mailbox size tiers

The current target operating model is tiered by mailbox size:

- below `10 GB`: a mailbox can remain in a shared mailbox database used by multiple mailboxes
- above `10 GB`: a mailbox should be moved to a dedicated database
- above `50 GB`: the mailbox storage layout should support a split strategy
- above `100 GB`: the mailbox should support online archive mode with degraded performance

These thresholds express the current architectural intent and may later become policy-driven, but the core principle is stable: mailbox placement must adapt to size and operational cost.

### Shared mailbox database tier

For smaller mailboxes, storing multiple mailboxes in the same mailbox database is the default model.

This tier is optimized for:

- efficient infrastructure usage
- simpler hosting for common mailbox sizes
- standard backup operations for groups of users

### Dedicated mailbox database tier

Once a mailbox grows beyond `10 GB`, it should be eligible for placement in a dedicated database.

The purpose of this tier is to:

- isolate backup scope
- simplify targeted restore procedures
- reduce the impact of very large mailboxes on other users
- make user-level migration more predictable

### Split strategy for very large mailboxes

Once a mailbox grows beyond `50 GB`, the architecture should support a split strategy.

This split is a technical partitioning mechanism that remains invisible to the end user.

Users must continue to experience a single logical mailbox even when the underlying storage is partitioned internally for operational reasons.

This means the mailbox is no longer treated as a single monolithic storage unit for all operational concerns. The architecture should allow one or more of the following:

- partitioning mailbox data into multiple logical or physical segments
- separating active and less-active message ranges
- distributing mailbox storage across dedicated data boundaries that remain manageable for backup and migration

The architectural goal is to prevent a very large mailbox from becoming an indivisible operational object.

### Online archive tier

Once a mailbox grows beyond `100 GB`, the architecture should support online archiving with degraded performance.

This archive tier should remain accessible to the user, but it does not need to provide the same latency or interaction quality as the active mailbox tier.

The purpose of the online archive tier is to:

- keep historical email accessible without requiring full cold export workflows
- control the cost of very large mailboxes
- preserve acceptable performance for the active mailbox
- provide a predictable retention model for long-lived email histories

Online archive mode should therefore be treated as a distinct storage and access class rather than as a transparent extension of the active mailbox.

### Backup and migration implications

The mailbox placement model is also driven by operational requirements.

The architecture should make it possible to:

- back up server-wide control and shared data independently from mailbox data
- back up groups of mailboxes independently from other groups
- move a single large mailbox without moving unrelated tenant data
- restore a mailbox or mailbox group with limited blast radius

This is one of the reasons the architecture should not assume a single `PostgreSQL` database for every mailbox and every platform concern.

### Mailbox import and export

`LPE` supports mailbox import and export in `PST` format.

This capability is part of the platform migration and interoperability strategy, especially for environments moving from legacy mail systems or requiring mailbox extraction for transfer workflows.

`PST` support must be treated as a controlled import-export capability around the internal mailbox model, not as a primary storage format or a substitute for the platform's native data architecture.

`PST` operations should support both:

- full mailbox import and export
- partial mailbox import and export

Partial operations are important for selective migration, legal extraction, archive workflows, and user-scoped recovery scenarios.

## Retention, Deletion, and Legal Hold

### Retention policy scope

Retention policy can be configured at:

- tenant level
- domain level
- mailbox level
- folder level

This allows lifecycle management to reflect both organizational and mailbox-specific requirements.

### Legal hold

Legal hold is configured at tenant level.

### Rights-driven interaction model

The interaction between retention, quarantine, online archive, export, and audit is governed through rights attribution.

This means the architecture should not assume one universal lifecycle behavior for every actor. Instead, visibility and allowed actions depend on explicitly assigned rights.

## Attachment Storage Principles

### Attachment deduplication inside the domain

To reduce unnecessary storage usage, attachments should be stored only once whenever the binary content is identical.

This means the internal data model should support:

- a logical distinction between message-to-mailbox delivery records and attachment blob storage
- multiple mailbox-visible message instances referencing the same stored attachment payload
- multiple different messages referencing the same stored attachment payload when the blob is identical
- storage optimization without losing per-mailbox message semantics

This rule is especially important for:

- internal multi-recipient deliveries where the same attachment would otherwise be duplicated across many mailboxes
- repeated transmission of the same document or file across different messages

The deduplication scope should therefore not be limited to one delivered message instance. It should be able to reuse the same stored attachment blob across different messages when content identity matches.

The architectural implication is that attachment blob storage should behave as a shared content-addressable or equivalently deduplicated storage layer, while message objects, mailbox placement, retention rules, and user-visible metadata remain logically independent.

The deduplication scope is limited to the domain.

Identical blobs may be deduplicated across different messages inside the same domain, but cross-domain deduplication is not part of the tenant-isolation model.

### Export behavior

Internal attachment deduplication must remain invisible to export consumers.

Mailbox export, including `PST` export, must be able to include attachments correctly in the exported mailbox content even when those attachments are stored only once internally.

The architecture therefore requires:

- storage-level deduplication internally
- message-level attachment reconstruction at export time
- no loss of attachment fidelity in full or partial exports
- no coupling between blob deduplication and user-visible mailbox ownership semantics

The current v1 implementation uses a domain-scoped shared blob store for attachment bytes, while keeping per-message attachment rows for mailbox projection, search metadata, and export reconstruction.

### Deletion, audit, archive, and integrity rules

When a message is deleted, the architecture should preserve auditability by reconstructing the message and storing it as deleted for audit purposes.

When a message is archived, the message must also be reconstructed together with its blobs as part of the archive process.

Blob integrity revalidation must be performed for the mailbox concerned before each archive operation.

These rules ensure that deduplicated storage does not undermine deletion traceability, archival completeness, or later evidentiary use.

## Search and Indexing Model

### Default search engine

Search is implemented in `PostgreSQL` by default.

### Future large-scale search engine

For larger infrastructures, a separate search engine may be introduced later.

That engine is intentionally left undefined at this stage and will be specified in a future architecture revision.

### Index scope

The architecture should support:

- per-mailbox indexes
- archive indexes
- text indexes for supported attachment content

The indexing model must remain compatible with tenant isolation, archive access, encryption-at-rest policy, and attachment deduplication constraints.

In the current v1 implementation, attachment search is backed by `PostgreSQL` full-text search over message text plus extracted attachment text for supported formats only: `PDF`, `DOCX`, and `ODT`.

## Roles and IAM Model

The architecture distinguishes the following roles:

- server administrator: manages server infrastructure, domains, and domain administrators
- domain administrator: manages users and mailboxes for the domain under its control
- transport operator: operates the sorting-center layer
- compliance and audit role: domain-scoped compliance and audit access
- support or helpdesk role: domain-scoped support access
- end user: web-interface user

Administrative authority must be aligned with tenant scope and operational separation between the core platform and the shared sorting layer.

## Administration UI Interaction Pattern

The default administration-console pattern for manageable collections is a full-width management list with a single primary `New` or `Create` action in the list header.

Selecting an existing item opens a right-side drawer-style modal containing the item's details and contextual actions. Creating a new item uses the same drawer pattern with an empty form. Persistent side-by-side create forms should be avoided for primary administration flows because they reduce list readability and make domain-scoped management harder to scan.

This pattern applies by default to `LPE` and `LPE-CT` administration screens, especially domains, administrators, mailbox accounts, filtering rules, and future list-based control-plane objects. Exceptions are acceptable only when the object is not list-oriented or when a specialized operational view is clearly more efficient.

## Recipient Privacy and `Bcc` Handling

### Core rule

`Bcc` data must never be treated as ordinary message content.

The architecture must guarantee that blind-copy recipient information is isolated from end-user-visible message data, mailbox search indexes, and AI retrieval surfaces unless an explicitly authorized administrative or transport-level workflow requires access.

### Separation of concerns

The internal model should distinguish at least between:

- transport-envelope recipient data
- message header and body content
- mailbox-visible recipient projections

`Bcc` belongs to transport and delivery metadata, not to the normal user-visible message representation.

This means a recipient opening a delivered message in a mailbox must not receive a representation that exposes the full blind-copy recipient list unless that visibility is explicitly part of the sender's own authorized mailbox view.

### Indexing and search constraints

Search systems, including standard mailbox search and future AI-assisted retrieval, must not index or expose `Bcc` data in ordinary user-facing retrieval paths.

The architecture should enforce this by ensuring:

- `Bcc` data is stored in protected metadata fields separate from searchable message content
- default search indexes exclude `Bcc`
- AI document preparation, chunking, embeddings, and retrieval inputs exclude `Bcc`
- result rendering layers cannot reconstruct or reveal `Bcc` from protected metadata in normal user flows

In other words, the safe default is structural exclusion, not just UI hiding.

### Per-mailbox projection model

Delivered message views should be projected per mailbox so that each mailbox sees only the recipient information it is allowed to know.

This projection model is especially important because content deduplication, shared blob storage, and future AI processing must not create an accidental side channel that reveals blind-copy recipients across mailbox boundaries.

### Administrative access

If `Bcc` data is retained for traceability, compliance, or transport investigation, access to it must be restricted to explicitly authorized operational workflows.

This access must be treated as privileged metadata access rather than as part of the normal mailbox search and reading model.

### Internal delivery-store retention

`Bcc` data should also be retained in the internal delivery store for audit and compliance purposes.

However, this retention must not change the core privacy rule. Even when `Bcc` is preserved in the internal delivery store:

- it remains protected delivery metadata
- it must not become part of ordinary mailbox-visible message data
- it must not be included in standard search indexes
- it must not be exposed to AI preparation or retrieval pipelines used for end-user features

The architecture must therefore support a dual requirement:

- retain `Bcc` for authorized audit, compliance, and investigation workflows
- prevent `Bcc` leakage into normal reading, search, export, or AI-assisted access paths unless explicitly authorized by policy

### Export policy for `Bcc`

`Bcc` handling in mailbox export must follow the same projection and authorization model as interactive access.

The architecture should therefore enforce the following rules:

- a sender-side mailbox export may include the sender's own `Bcc` data when that data is part of the sender's authorized mailbox view
- a recipient-side mailbox export must not reveal blind-copy recipients that are not visible in that recipient's authorized mailbox view
- administrative, audit, compliance, or transport-investigation exports may include `Bcc` only through explicitly privileged workflows governed by policy

This rule applies to both full and partial exports, including `PST` export.

The core principle is that export must reproduce the authorized mailbox projection, not the full protected delivery metadata set, unless the export itself is a privileged compliance operation.

## Deferred Architecture Topics

The following architecture topics are recognized but intentionally left for later definition:

- high availability and degraded-mode behavior
- technical logging model
- metrics model
- correlation model around the unique message identifier
- administrative action journaling

## MVP Direction

The current architectural direction for the first product phases is:

- hosted mailbox management for controlled domains
- inbound mail intake through DMZ sorting centers
- outbound mail relay through DMZ sorting centers
- traceability and quarantine as first-class capabilities
- `JMAP` as the main modern API surface
- a first `JMAP Mail` MVP with real session capabilities, `Mailbox/get`, `Email/query`, `Email/get`, draft-only `Email/set`, and canonical `EmailSubmission/set`
- `IMAP` as a mailbox-access compatibility layer
- an initial `ActiveSync` MVP adapter with `Provision`, `FolderSync`, `Sync`, and canonical `SendMail`
- an initial `CardDAV` and `CalDAV` MVP adapter for contacts and calendar compatibility
- `EWS` as a future extension after stabilization of the canonical submission and synchronization model
- `PST` mailbox import and export for migration and interoperability
- collaboration services for contacts, calendars, and to-do lists
- mailbox growth management through storage tiers, dedicated databases, split-capable large mailbox handling, and online archive support

The precise supported `JMAP Mail` MVP scope and its intentional limitations are documented in `docs/architecture/jmap-mail-mvp.md`.

The precise supported `ActiveSync` MVP scope and its intentional limitations are documented in `docs/architecture/activesync-mvp.md`.
