# Initial Architecture

### Starting decisions

- primary store: `PostgreSQL`
- modern protocol axis: `JMAP`
- initial compatibility through `0.1.2`: `IMAP`, which remains a permanently supported mailbox-access communication protocol; exposed `SMTP` transport handled by the `LPE-CT` sorting center
- current `0.1.3` Exchange compatibility focus: `EWS`, implemented without moving `SMTP` or canonical mailbox state out of `LPE`
- `MAPI over HTTP` implementation starts as guarded route and authentication wiring for future Outlook desktop support, with autodiscover publication limited to an explicit interoperability-test switch
- LPE code: `Apache-2.0`
- dependencies: prefer `Apache-2.0`, allow `MIT` only with a documented exception
- data architecture prepared for future local AI
- distinct `LPE-CT` sorting center for exposed inbound and outbound `SMTP` transport, deployable in the `DMZ` without moving the business core

### Vision

`LPE` is a modern mail and collaboration server. The business core must not depend on `IMAP` or `SMTP`. External protocols are adapters around a stable internal model.

Native clients remain an important goal. A user must be able to connect an `LPE` mailbox from a compatible mobile or desktop client, for example the iPhone Mail application, without losing mailbox consistency across protocols.

This implies that every supported client submission path, especially `JMAP`, `IMAP`, or `ActiveSync`, feeds the same canonical message representation in `LPE`, including the authoritative `Sent` mailbox view. Inbound and outbound `SMTP` transport execution remains a sorting-center responsibility.

`ActiveSync` is the first targeted mobile/native compatibility layer for clients that actually support `Exchange ActiveSync`, such as Outlook mobile and iOS mail clients. Outlook for Windows desktop must not be forced into `ActiveSync` as an Exchange account. `IMAP` was the development-start compatibility path through `0.1.2` and remains a supported mailbox-access communication protocol for desktop and other IMAP clients. Release `0.1.3` moves the Exchange-style desktop compatibility focus to the `EWS` adapter while starting guarded `MAPI over HTTP` foundation work for the future Outlook desktop Exchange route. `MAPI` autodiscover is available only through an explicit administrator test switch while Outlook login is still being built.

`CalDAV` and `CardDAV` are standards-based compatibility adapters for collaboration data. They must remain layered over the canonical `LPE` contact and calendar models, without introducing a separate DAV storage or rights model.

Tasks and to-do items must follow the same rule: one canonical task model in `LPE`, stored in the database first, then reused later by `JMAP Tasks`, `DAV`, and mobile layers without any parallel business model.

Mailbox `Sieve` rules follow the same rule. Scripts are stored per account in `LPE`, administered through `ManageSieve`, and executed during final inbound delivery inside the core `LPE` runtime without moving sorting-center edge policy into the core or introducing a parallel routing engine.

The initial submission model is transactional in the `LPE` core and exposed by `/api/mail/messages/submit`:

1. verify the submitting account
2. ensure the `Sent` mailbox exists
3. create the canonical message in `messages`
4. store visible recipients in `message_recipients` and retain `Bcc` in separate protected storage
5. index the body in `message_bodies` without including `Bcc` in `participants_normalized`
6. add an `outbound_message_queue` entry for handoff through `LPE-CT`
7. record the action in `audit_events`

This sequence makes `Sent` authoritative before the sorting center performs the actual SMTP delivery.

The functional v1 integration between the core platform and the sorting center is now explicit:

- an `LPE` worker reads `outbound_message_queue` and calls `LPE-CT`
- `LPE-CT` returns a structured transport result with at least one status among `queued`, `relayed`, `deferred`, `quarantined`, `bounced`, and `failed`
- `LPE-CT` delivers accepted inbound messages into `LPE` through an internal final-delivery API

For outbound transport, `LPE-CT` remains responsible for the advanced edge MTA functions:

- classifying SMTP failures into transient retry, bounce/`DSN`, or permanent failure
- local outbound routing rules
- outbound throttling policies
- detailed technical status for the latest attempt, persisted on the `LPE` side without moving SMTP logic into the core

The detailed contract is documented in `docs/architecture/lpe-ct-integration.md`.

All client layers must use this canonical submission and synchronization model. No client layer may write its own parallel `Sent` or `Outbox` logic.

Every file entering through an external connection or through a client must be validated with Google `Magika` before normal processing. This applies to `LPE-CT` for external ingress paths and to `LPE` for client-side uploads and imports.

The currently implemented `JMAP Mail` MVP in `lpe-jmap` follows that rule. `EmailSubmission/set` does not speak `SMTP`; it reuses the existing canonical submission workflow after loading a persisted draft. `Mailbox/get`, `Email/query`, and `Email/get` read the canonical mailbox projection without reinjecting `Bcc` into standard search paths. `JMAP` over WebSocket now reuses those same canonical projections and state tokens for real-time refresh, with `PostgreSQL` `LISTEN` / `NOTIFY` waking the adapter after canonical commits and mailbox-delegation-aware push state covering owned plus delegated mailboxes without introducing a second mailbox-state engine. The supported scope is detailed in `docs/architecture/jmap-mail-mvp.md`.

The currently implemented `IMAP` MVP in `lpe-imap` follows the same rule. `LOGIN`, `LIST` / `XLIST`, `STATUS`, flat mailbox management, `SELECT` / `EXAMINE`, `CHECK`, `CLOSE`, `UNSELECT`, `EXPUNGE`, `FETCH`, `STORE`, richer `SEARCH`, `COPY`, and `UID` read and update canonical mailbox state, while system folder aliases such as `Deleted Items` and `Trash` converge on the canonical trash mailbox and `APPEND` reuses canonical draft or import persistence without introducing parallel `Sent`, `Drafts`, or `Outbox` logic. The supported scope and current UID/sync tradeoffs are detailed in `docs/architecture/imap-mvp.md`.

The current `Sieve` / `ManageSieve` MVP follows the same rule. `ManageSieve` only manages per-account stored scripts; execution happens during final inbound delivery and stays bounded to mailbox actions such as `fileinto`, `discard`, `redirect`, and `vacation`. `redirect` and `vacation` reuse canonical `LPE` submission and outbound relay through `LPE-CT` instead of introducing a parallel transport engine. The supported scope is detailed in `docs/architecture/sieve-managesieve-mvp.md`.

The current `ActiveSync` MVP in `lpe-activesync` follows the same rule. `Provision`, `FolderSync`, `Sync`, and `SendMail` are implemented as an adapter over the same account authentication, draft persistence, mailbox synchronization, and canonical submission model. `SendMail` does not bypass the core mailbox workflow or `LPE-CT`; it reuses the canonical submission path so the authoritative `Sent` copy exists before outbound relay. The supported scope is detailed in `docs/architecture/activesync-mvp.md`.

The `0.1.3` `EWS` adapter in `lpe-exchange` follows the same rule for mailbox, contacts, and calendar synchronization. `FindFolder`, `GetFolder`, `SyncFolderHierarchy`, `FindItem`, `GetItem`, `SyncFolderItems`, selected `CreateItem`, selected `DeleteItem`, `CreateFolder`, and `DeleteFolder` reuse canonical `LPE` storage and submission paths without introducing Exchange-specific storage or rights. It is not a complete Exchange server. `MAPI over HTTP` currently has authenticated route wiring, session bootstrap, and read-only mailbox-folder bootstrap ROPs; autodiscover publication remains opt-in for interoperability testing. The supported scope is detailed in `docs/architecture/ews-mapi-mvp.md`.

Client auto-configuration must publish only real endpoints. In v1, `Thunderbird` and Outlook for Windows desktop may receive `IMAP` settings and must advertise `SMTP` submission only when an authenticated client-submission endpoint is explicitly exposed; the internal `LPE -> LPE-CT` relay must never be described as a client-submission service. Outlook autodiscover must not advertise `ActiveSync` as a desktop Exchange route. `EWS` may be advertised only when the administrator explicitly enables the narrow contacts/calendar EWS endpoint and accepts its MVP limits. `MAPI over HTTP` autodiscover publication is allowed only when `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled for interoperability testing; it must be treated as experimental until real Outlook desktop login succeeds.

The current `DAV` MVP in `lpe-dav` follows the same adapter approach for collaboration compatibility. `CardDAV`, `CalDAV`, and the first `VTODO` layer reuse the same mailbox-account authentication, expose canonical `contacts`, `calendar_events`, and `tasks` through a minimal DAV collection model, and update those canonical tables directly instead of introducing DAV-only business logic. The supported scope is detailed in `docs/architecture/dav-mvp.md`.

The current `JMAP Contacts` and `JMAP Calendars` MVP in `lpe-jmap` follows the same approach. `AddressBook`, `ContactCard`, `Calendar`, and `CalendarEvent` are exposed above the canonical `contacts` and `calendar_events` tables, with one canonical `default` address book and one canonical `default` calendar per account, rights bounded by the authenticated account, and no parallel storage or business logic. The supported scope is detailed in `docs/architecture/jmap-contacts-calendars-mvp.md`.

The current `tasks` MVP follows the same canonical approach. Personal tasks are stored in `tasks`, exposed through account-scoped `/api/mail/tasks` endpoints, and included in `/api/mail/workspace` so future `JMAP Tasks`, `DAV`, and mobile adapters can reuse the same base. The supported scope is detailed in `docs/architecture/tasks-mvp.md`.

The current `JMAP Tasks` MVP in `lpe-jmap` follows that same adapter rule. `Task` reads and writes map directly to the canonical `tasks` table, one canonical `default` `TaskList` is exposed per account, rights remain limited to the authenticated account, and `Task/changes` plus `Task/queryChanges` use canonical `updated_at` and `sort_order` without introducing a `JMAP`-specific sync store. The supported scope is detailed in `docs/architecture/tasks-mvp.md`.

The webmail uses account authentication separate from administration. The `/mail/` form calls `/api/mail/auth/login`, which verifies the `argon2` hash stored in `account_credentials`, optionally requires a mailbox `TOTP` code when a verified factor exists, creates a session in `account_sessions`, and exposes the identity through `/api/mail/auth/me`. Mailbox `OIDC` now provides a second interactive login path that still ends in the same internal mailbox session model.

The webmail must not display mock datasets, demo marketing copy, or nonfunctional placeholder actions in a functional environment. After authentication, it loads user state through `/api/mail/workspace`, which exposes persistent messages, contacts, events, and tasks for the account. Submission, drafts, contacts, calendar entries, and tasks go through authenticated endpoints so the client remains aligned with the canonical `LPE` model. Contact and calendar create, update, and delete actions use the authenticated account APIs and canonical collection rights rather than client-local state. Drafts are persistent messages in the `Drafts` mailbox; editing updates the same row, sending creates the authoritative `Sent` copy and then removes the `Drafts` copy, and deletion is limited to a message owned by the authenticated account in the `Drafts` mailbox.

### Main building blocks

1. `lpe-domain`
Shared business types.

2. `lpe-core`
Application rules and domain orchestration.

3. `lpe-storage`
`PostgreSQL` persistence adapter, later blob storage, and shared mail parsing helpers (`RFC822`, headers, addresses) reused by import paths and protocol adapters when they need to rebuild the canonical model without duplicating parsing logic.

4. `lpe-ai`
Contracts and services for future local AI with provenance.

5. `lpe-jmap`
Modern entry point for the web client and future native apps.

6. `lpe-admin-api`
Control plane for the back office.

7. `lpe-cli`
Local server executable.

8. `nginx` on Debian
HTTP front end used to expose the static administration UI and reverse-proxy `/api/` to `lpe-admin-api`.

9. `LPE-CT` in the DMZ
Separate sorting center for exposed `SMTP` ingress, outbound relay, perimeter filtering, quarantine, and controlled relay toward the core `LPE` services on the `LAN`.

### MVP priorities

- accounts, domains, aliases, quotas
- IMAP compatibility, established through `0.1.2` and permanently supported as a mailbox-access communication protocol
- inbound and outbound `SMTP` transport through `LPE-CT`
- mobile/native compatibility through `ActiveSync` as the first target for clients that support `Exchange ActiveSync`
- Outlook for Windows desktop compatibility through the supported `IMAP` path when configured that way, with `0.1.3` now implementing `EWS` as an additional Exchange-style compatibility path and starting non-advertised `MAPI over HTTP` groundwork
- contacts and calendar compatibility through `CardDAV` and `CalDAV`
- a canonical personal-tasks model prepared for future `JMAP Tasks`, `DAV`, and mobile adapters
- `EWS` implementation in `0.1.3`, bounded by canonical submission and synchronization rules
- sent-message consistency across client protocols and the `Sent` view
- HTTPS webmail
- search
- web administration
- document projections and local AI artifacts
- attachment indexing for `PDF`, `DOCX`, and `ODT`
- multilingual web interfaces for `en`, `fr`, `de`, `it`, `es`

### Current protocol-completion phase

The current delivery phase is to finish the existing protocol adapters while beginning the minimal `MAPI over HTTP` foundation required for Outlook desktop. `IMAP` brought the compatibility foundation through `0.1.2` and remains supported as a mailbox-access communication protocol; `0.1.3` focuses on the already-selected `EWS` adapter and must keep early MAPI routes non-advertised until real semantics exist.

This phase is explicitly depth-first:

- `JMAP`: complete canonical `state` and `changes` semantics, WebSocket reliability, mailbox delegation behavior, and shared collection consistency
- `IMAP`: keep improving synchronization correctness, `UID` behavior, flag handling, and compatibility coverage under realistic mailbox operations as continuing support for the `0.1.2` compatibility foundation
- `ActiveSync`: treat Outlook mobile and iOS compatibility as a strategic flagship, with emphasis on long-poll stability, send-flow correctness, and `FolderSync` plus `Sync` edge cases
- `EWS`: implement and stabilize Exchange-style mailbox, contacts, and calendar synchronization in `0.1.3`
- `MAPI over HTTP`: keep initial route, authentication, EMSMDB, NSPI, and session-context work behind non-advertised endpoints until real Outlook desktop login is viable
- `DAV`: focus on `CardDAV`, `CalDAV`, and `VTODO` correctness plus client-matrix interoperability instead of broader DAV surface expansion
- `ManageSieve` and mailbox `Sieve`: focus on script correctness, canonical execution during final delivery, and interoperability of mailbox-side filtering rather than additional extension breadth

During this phase, protocol work should prefer real-client interoperability tests, deterministic canonical-state behavior, and long-lived sync reliability over new exposed protocol features.


