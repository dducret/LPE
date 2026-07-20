# LPE Development Agent Context

This file defines the minimum context any AI agent must use when changing `LPE`.

Don't assume. Don't hide confusion. Surface tradeoffs.
 Before implementing:
   State your assumptions explicitly. If uncertain, ask.
   If multiple interpretations exist, present them - don't pick silently.
   If a simpler approach exists, say so. Push back when warranted.
   If something is unclear, stop. Name what's confusing. Ask.

Minimum code that solves.
   No features beyond what was asked.
   No abstractions for single-use code.
   No "flexibility" or "configurability" that wasn't requested.
   No error handling for impossible scenarios.
   If you write 200 lines and it could be 50, rewrite it.
   Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify. the problem. Nothing speculative.
 
Touch only what you must. Clean up only your own mess.
	When editing existing code:
   Don't "improve" adjacent code, comments, or formatting.
   Don't refactor things that aren't broken.
   Match existing style, even if you'd do it differently.
   If you notice unrelated dead code, mention it - don't delete it.
 When your changes create orphans:
   Remove imports/variables/functions that YOUR changes made unused.
   Don't remove pre-existing dead code unless asked.
   The test: Every changed line should trace directly to the user's request.

Define sucess criteria. Loop until verified.
 Transform tasks into verifiable goals:
   "Add validation" → "Write tests for invalid inputs, then make them pass"
   "Fix the bug" → "Write a test that reproduces it, then make it pass"
   "Refactor X" → "Ensure tests pass before and after"
 For multi-step tasks, state a brief plan:
   1. [Step] → verify: [check]
   2. [Step] → verify: [check]
   3. [Step] → verify: [check]
 Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

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

## Microsoft Protocol Documentation

For Microsoft protocol, Outlook, Exchange, EWS, ActiveSync, Autodiscover, NSPI,
EMSMDB, ROP, MAPI over HTTP, ICS, FastTransfer, and related interoperability
work, query official Microsoft Learn Open Specifications first.

Use `docs/microsoft/` as the project reference area:

- check `docs/microsoft/protocol-sources.toml` before downloading references
- keep cached Microsoft PDFs, DOCX files, HTML exports, ZIP files, and extracted
  full-text copies under `docs/microsoft/cache/`
- record the official Learn URL, download URL, publication version or date,
  local cache path, SHA256, and exact sections relied on in
  `docs/microsoft/protocol-sources.toml`
- do not commit Microsoft protocol documents or extracted full-text copies
  outside the cache, and do not commit cached copies unless redistribution has
  been explicitly reviewed and approved
- cite Microsoft documents by protocol ID and section in LPE code comments,
  tests, and architecture notes instead of copying long specification text

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
- finish current protocol depth and interoperability before adding new protocol breadth
- prioritize protocol completion depth before protocol breadth: `JMAP`, `IMAP`, `ActiveSync`, the active `EWS` compatibility adapter, full `MAPI over HTTP` Outlook desktop compatibility, `DAV`, then `ManageSieve` / mailbox `Sieve`
- internet-facing `SMTP` must stay in `LPE-CT`, not move back into the core `LPE` server
- client autodiscovery and autoconfiguration must publish only endpoints that are truly implemented and exposed
- top-level Outlook `EXPR` autodiscover metadata is permitted only for the documented Outlook Anywhere / RPC over HTTP implementation path and must stay aligned with real `/rpc/rpcproxy.dll` mailbox transport behavior
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

`LPE-CT` may use dedicated local technical data stores, including a local database, only for perimeter-owned operational state such as Bayesian filtering, reputation, greylisting, quarantine indexes, and cluster coordination. Those stores must never become canonical mailbox, collaboration, rights, or user-visible state.

## Outlook and Native Client Rules

Native Outlook and mobile support is a first-class requirement.

- `ActiveSync` targets mobile and native clients that actually support `Exchange ActiveSync`; do not try to force Outlook for Windows desktop to use `ActiveSync` as an Exchange account
- Outlook for Windows desktop compatibility is a main project goal; implement the Exchange-compatible `MAPI over HTTP`, `EWS`, Autodiscover, EMSMDB, NSPI, profile, view, synchronization, send, and reconnect behavior Outlook requires
- protocol planning must treat both Outlook desktop `IMAP` interoperability and `ActiveSync` mobile compatibility labs as flagship requirements before introducing new client protocols
- `EWS` is the active Exchange compatibility adapter; widen it as needed for real Outlook and Exchange-compatible client behavior while keeping canonical mailbox, contacts, calendar, task, and submission state in `LPE`
- `MAPI over HTTP` is the primary Outlook desktop Exchange route; implement all behavior needed for Outlook 2016, Outlook 2019, and supported Microsoft 365 Apps cached-mode interoperability, keep it behind authenticated endpoints and opt-in autodiscover publication until Outlook desktop profile creation, EMSMDB, NSPI, session context, and canonical mailbox synchronization are proven in interoperability testing, and treat Outlook Anywhere / RPC over HTTP as a later legacy compatibility shim for `EXPR` publication rather than the first implementation path
- single-node sticky MAPI session state is acceptable for the first Outlook 2016 / 2019 lab gate; cross-process session replay and load-balanced failover are production hardening
- `IMAP` + `SMTP` + autodiscover is the current Outlook desktop path, but must not be treated as the final Outlook adoption story
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
- for Outlook interoperability work, do not build a local release binary for deployment; the user recompiles and deploys LPE from source on the server at `192.168.1.28`
- do not contradict documented architecture choices without updating the documentation explicitly
- if a change affects behavior, prerequisites, installation, release framing, or architecture, update the relevant documentation in the same work
- if a new durable rule appears, update `AGENTS.md`
- tests should use realistic parameters and protocol builders; fixed literals belong only to deterministic fixtures, timestamps, IDs, or golden vectors
- `cargo test` accepts only one test-name filter argument; when running multiple focused Rust tests, run separate `cargo test` commands or use a single broader module/prefix filter
- patches must be correctly implemented, API-compliant, and integrated through the existing architecture; do not ship trace-specific workarounds when a documented protocol rule, shared abstraction, canonical data path, or existing subsystem contract should own the behavior
- when protocol work is planned, prefer correctness, state consistency, long-lived sync reliability, and real-client interoperability testing over introducing additional protocol surface area
- prefer explicit architectural documentation over leaving structural assumptions implicit in code
- keep ad hoc text state columns with table-level `CHECK` constraints while state machines are still changing; replace them with PostgreSQL enums only after state churn settles and the migration semantics are worth the added rigidity
- keep lightweight message-level thread identifiers until thread lifecycle, MAPI conversation IDs, or JMAP `Thread/changes` require first-class stable thread identity; when that threshold is crossed, add a real `threads` table instead of stretching mailbox-message summary fields into a thread store
- keep the current simple normalized email/domain checks until internationalized mailboxes become in-scope; when EAI/IDNA local-part or domain behavior is introduced, add generated normalized email/domain/local-part helpers instead of duplicating ad hoc string normalization across schema and runtime code
- for Rust crates, `lib.rs` must act only as a central hub for module declarations, re-exports, and minimal crate wiring; do not add implementation code to `lib.rs` when that code can be placed in helper modules
- for Rust crates, `services.rs` must act only as a central hub for module declarations, re-exports, and minimal crate wiring; do not add implementation code to `services.rs` when that code can be placed in helper modules
- for Rust crates, `mapi.rs` must act only as a central hub for module declarations, re-exports, and minimal crate wiring; do not add implementation code to `mapi.rs` when that code can be placed in helper modules
- production source files should stay below 1,500 lines; thousand-line source files require an explicit split plan before adding more behavior, with exceptions only for generated files or dense protocol tables when the justification is documented
- use a line-count scan such as `git ls-files | rg '\.(rs|js|ts|tsx|jsx|css|html|sql)$' | % { $lines = (Get-Content $_ | Measure-Object -Line).Lines; if ($lines -gt 1500) { [pscustomobject]@{Lines=$lines; File=$_} } } | Sort-Object Lines -Descending` to find oversized production source files before expanding large modules
- keep demo data, mock content, placeholder marketing copy, and nonfunctional placeholder actions out of runtime UI, published API responses, published configuration, and bootstrap product state; confine them to tests or documentation previews only
- for MAPI over HTTP Outlook compatibility, implement whatever real Outlook requires for profile creation, mailbox views, associated contents, hierarchy, synchronization, rules, configuration messages, special folders, cached mode, reconnect, send, and shutdown behavior; do not keep local compatibility restrictions that block Outlook unless Microsoft protocol documentation, real Outlook traces, or canonical `LPE` state/security requirements prove they are necessary
- for frontend work, converge on the shared Tailwind-based design system instead of one-off utility sprawl
- for administration UI lists in `LPE` and `LPE-CT`, use the default management pattern: full-width list, primary `New` or `Create` action in the list header, and a right-side drawer for creation, details, and contextual actions

## Installation Scope

- the initial Linux deployment target is `Debian Trixie`
- installation scripts must first target deployment from the Git repository
- Windows Server support is deferred and must not be assumed in Linux scripts
- new `LPE` `0.5.0` deployments start from an empty SQL database
- upgrades from pre-0.5 releases are unsupported; `update-lpe.sh` must reject
  their schema versions without mutation
- during 0.5.0 interoperability testing, an explicitly reviewed 0.5.x schema
  fix may use a forward-only, transactional, idempotent SQL update for the
  existing `0.5.0-sql-v1` label; `update-lpe.sh` must reject pre-0.5 schemas
  before running it, `schema.sql` remains canonical for new databases, and the
  matching architecture and installation documentation must change with it
