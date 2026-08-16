# Master Codex Prompt: Microsoft Protocol P0/P1 Backlog

Copy the prompt below into a Codex task.

## Prompt

You are working in C:\Development\LPE. Address all 15 P0/P1 entries in docs/architecture/microsoft-protocol-gap-backlog.md. The companion docs/architecture/microsoft-protocol-gap-backlog-prompts.md is the authoritative detailed prompt bank: follow its Shared Instructions and the numbered prompt matching each backlog entry.

Your outcome is not generic Exchange parity. For every implement-now entry, close the evidenced gap within LPE's documented bounded canonical architecture. For every needs-trace or keep-explicitly-unsupported entry, satisfy its required evidence gate or explicitly retain and test the current boundary. Do not silently implement deferred protocol breadth.

Before editing:

1. Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md, docs/architecture/microsoft-protocol-gap-backlog.md, docs/architecture/microsoft-protocol-constants-gap.md, docs/architecture/microsoft-protocol-gap-backlog-prompts.md, docs/microsoft/protocol-sources.toml, and only architecture documents directly relevant to the active phase.
2. Inspect the current implementation, tests, schema, and client endpoint publication state. State concise assumptions, a staged plan, and success criteria. Treat the backlog Decision and Required evidence columns as requirements.
3. For protocol changes, consult official Microsoft Learn Open Specifications first. Cite the protocol ID and exact sections in code, tests, and relevant documentation.

Core rules:

- LPE is canonical for mailboxes, collaboration, rights, user-visible state, authoritative Sent, and submission records. LPE-CT is canonical for Internet-facing SMTP, filtering, quarantine, and perimeter traceability.
- Do not create Exchange-local mailbox, Sent, Outbox, directory, rule, OOF, subscription, synchronization, or delegation truth. No adapter can bypass canonical submission or synchronization.
- Keep Bcc protected: never expose or index it in normal search, sync manifests, user projections, or AI.
- Autodiscover must advertise only an actually implemented, authenticated, LPE-CT-exposed route. Do not publish EXPR/RPC, MAPI, or SMTP based on metadata alone.
- Preserve strict MAPI/HTTP handle, object-type, session-context, ROP response-layout, version, and ICS contracts. Do not weaken an incompatibility gate unless Microsoft documentation, real Outlook evidence, and canonical security/state requirements justify it.
- Validate client files with Magika. Check dependencies against LICENSE.md.
- The test database is disposable. If schema work is genuinely necessary, directly update crates/lpe-storage/sql/schema.sql, matching schema-contract tests, and relevant architecture docs. Do not create an upgrade/migration script and do not print, store, or commit database credentials.

Execute in this order, finishing and verifying each phase before expanding scope:

1. MAPI/HTTP core Outlook path: G013, G043-G051, G116-G118.
2. MAPI named properties and canonical object projections: G027, G055-G064.
3. NSPI/EWS address-book projection: G014, G017-G018, G104, G119.
4. EWS core contract: G069-G078, G086, G104, G106-G108, G120. For operation-level scope use docs/architecture/ews-p0-p1-fix-prompts.md.
5. EWS canonical service families: G033-G034, G036, G075, G087-G092, G094-G102, G104, G109.
6. ActiveSync mobile interoperability: G001-G009.
7. Autodiscover and Outlook response shapes: G110-G111.
8. LPE-CT SMTP authenticated-submission boundary: G115.
9. Spam/phishing metadata boundary: G029.
10. Evidence gates: G052-G054 body/MIME/iCalendar; G065-G068 RSS/document/delegate/free-busy; G112 Outlook Anywhere RPC/HTTP; G103 EWS push; G113 IMAP NTLM/delegate extensions; and G067-G068 protected delegate/free-busy semantics.

For each active phase:

1. Locate the specific gap in code and reproduce it with a focused test, trace fixture, or deterministically documented evidence gap.
2. Make the smallest canonical change that closes it. Keep one owner for durable state and use atomic writes and canonical change/audit/tombstone paths.
3. Add regression tests for successful behavior, malformed or unsupported input, tenant/authorization isolation, no partial write, and cross-protocol visibility.
4. Run the smallest named test alias or a single cargo test filter, then the affected crate's aggregate gate. For schema changes, run matching schema-contract and TEST_DATABASE_URL-backed tests.
5. Update only directly affected architecture/protocol documentation, including the backlog evidence status and protocol sources when required.
6. Report the exact change, remaining intentional bounds, test commands/results, and protocol IDs/sections. Do not proceed to an unrelated feature while a phase has a regression.

For every needs-trace entry, first collect the specified sanitized Outlook, EWS, ActiveSync, IMAP, Microsoft RCA, or threat-model evidence. If evidence proves a supported workflow is blocked, add only the minimal behavior and fixture. If evidence is not available, do not manufacture it or broaden scope: add/retain a precise test of the advertised boundary and report the evidence gate as outstanding.

Completion criteria:

- Every implement-now P0/P1 entry has either an evidence-backed minimal implementation with focused tests or a documented, architecture-approved reason that no code change is currently needed.
- Every needs-trace or keep-explicitly-unsupported P1 entry has its required evidence artifact and a minimal plan, or a tested retained boundary explaining why the gate is still open.
- No endpoint is newly advertised unless its dedicated publication gate, edge checks, Microsoft RCA where required, and real-client evidence all pass.
- All changed code and schema pass the focused and aggregate tests appropriate to the affected crates.
- Finish with a table covering all 15 prompt-bank entries: gap rows, decision, implementation/evidence result, tests, documentation changed, and remaining gate.

Do not declare the task complete merely because the code compiles or a generic test suite passes. Completion is the evidence-backed disposition of all 15 entries under the documented LPE architecture.

