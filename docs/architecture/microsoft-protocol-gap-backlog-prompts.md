# Microsoft Protocol P0/P1 Gap-Fix Prompts

This prompt bank covers every P0/P1 entry in docs/architecture/microsoft-protocol-gap-backlog.md. Follow the entry's Decision: implement implement-now work; obtain the stated evidence before needs-trace work; and preserve a keep-explicitly-unsupported boundary until its evidence gate is met. These prompts never authorize endpoint publication.

## Shared Instructions

Prepend these instructions to each prompt.

Work in C:\Development\LPE. Address only the stated backlog entry and gap rows. Before editing, read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md, docs/architecture/microsoft-protocol-gap-backlog.md, docs/architecture/microsoft-protocol-constants-gap.md, directly relevant architecture documents, and docs/microsoft/protocol-sources.toml. Inspect implementation and focused tests first. State assumptions, a plan, and measurable success criteria.

Use official Microsoft Learn Open Specifications and cite protocol ID plus exact section in code, tests, and architecture notes. Record source metadata before downloading a new Microsoft document. LPE owns canonical mailbox, collaboration, rights, and user-visible state; LPE-CT owns Internet-facing SMTP, filtering, quarantine, and perimeter security. Do not create Exchange-local mailbox, Sent, Outbox, directory, rules, sync, or delegation truth. Keep Bcc out of search, sync manifests, user-facing projections, and AI; validate every client-provided file with Magika.

If schema work is genuinely required, update crates/lpe-storage/sql/schema.sql, matching schema-contract tests, and relevant architecture docs directly. The database is test-only, so do not create an upgrade/migration script. Never print or commit credentials. Review new dependencies against LICENSE.md.

Add focused tests for success, tenant/authorization isolation, malformed or unsupported input, no partial writes, and canonical cross-protocol effects. Run the smallest relevant test filter/alias, then the affected aggregate gate. Do not build a deployment binary. For needs-trace or explicitly-unsupported work, finish with either an evidence-backed minimal plan or a verified retained boundary; do not claim a fix without required evidence.

## 1. P0 MAPI/HTTP Core Outlook Path

Apply the shared instructions. Address G013, G043-G051, and G116-G118: MAPI/HTTP framing, ROPs, tables, folders, special folders, permissions, notifications, logon, property types, FastTransfer/ICS, and Outlook configuration affecting profile bootstrap, cached sync, send, reconnect, views, and FAI. Work in lpe-exchange MAPI/HTTP transport, session, ROP, dispatch, sync, table, property, and store layers. Follow MS-OXCMAPIHTTP 2.2.3.3.1/2.2.4, MS-OXCROPS 2.2, MS-OXCDATA 2.8, MS-OXCFXICS 2.2.4, and MS-OXOCFG 4.1-4.4.

Acceptance: golden tests cover logon/reconnect, cached hierarchy/contents sync, special folders, views/FAI, canonical submission/Sent, permissions, notifications, and FastTransfer/ICS. Local RCA readiness, Microsoft RCA, and separate Outlook 2016/2019 cached-mode evidence agree. Update the MAPI plan and cached-mode evidence template. Do not publish RPC/HTTP or relax Autodiscover.

## 2. P0 MAPI Named Properties And Object Projection

Apply the shared instructions. Address G027 and G055-G064: named-property mapping and Outlook mail, flag, rule, calendar, contact, task, note, journal, reminder, and post properties. Use MS-OXCPRPT 3.1.4.1, MS-OXOMSG 2.2, MS-OXOFLAG 2.2, MS-OXOCAL 2.2, MS-OXOCNTC 2.2, and MS-OXOTASK 2.2.

Add only trace-proven stable IDs and bounded canonical projections. Properties without a canonical model stay absent or parseably unsupported; opaque Exchange blobs never become active behavior. Verify Outlook-used property sets remain stable across profile, sync, compose, and reconnect; mutations reach canonical state and relevant JMAP/IMAP/EWS projections; invalid properties preserve exact ROP errors. Update full-object execution documentation for new canonical fields.

## 3. P1 Trace Gate: Body, MIME, And iCalendar Conversion

Apply the shared instructions. Address needs-trace G052-G054. Obtain a minimized, sanitized Outlook/EWS/ActiveSync trace proving a missing best-body, MIME-generation, or iCalendar shape blocks a supported workflow. Use MS-OXBBODY 2.1, MS-OXCMAIL 2.1, and MS-OXCICAL 2.1.

Turn the trace into a golden regression fixture. If proven, add only the minimal canonical-body/MIME/calendar conversion; if not, preserve bounded behavior. Verify Bcc-safe stable text/HTML/MIME/calendar output, attachment handling, and recurrence/time-zone behavior. Update EWS/ActiveSync architecture only if the supported conversion contract changes.

## 4. P1 Trace Gate: RSS, Document, Delegate, And Public-Folder Free/Busy

Apply the shared instructions. Address needs-trace G065-G068. Gather real Outlook setup, cached-mode, or scheduling traces for RSS, document messages, public-folder free/busy, and delegate-information objects. Use MS-OXORSS 2.2, MS-OXODOC 2.2, MS-OXOPFFB 2.2, and MS-OXODLGT 2.2.

For a proven delegate/free-busy requirement, use only canonical grants, sender rights, delegate_preferences, and calendar state. RSS/document work needs its own canonical-model decision. Store sanitized trace fixtures; test permission visibility and private-detail suppression. Otherwise retain the boundary; never create an Exchange object store or leak MAPI identity via REST/JMAP.

## 5. P1 Trace Gate: Outlook Anywhere RPC/HTTP

Apply the shared instructions. Address needs-trace G112. MAPI over HTTP remains first. Do not publish EXPR or expose rpcproxy.dll until Microsoft RCA Outlook Anywhere evidence and a real legacy Outlook profile prove authenticated mailbox transport for the same host. Inspect RPC proxy, LPE-CT routing, Autodiscover, and tests; use the MS-OXCRPC EMSMDB sections recorded by tests.

Any trace-proven implementation must be real authenticated RPC/HTTP transport, not an HTTP-authentication facade. Acceptance requires local RPC tests, edge checks, RCA, and legacy profile evidence for auth, channels, EMSMDB traffic, failure codes, and reconnect. Without it, retain the unpublished EXPR gate.

## 6. P0 EWS Core Contract

Apply the shared instructions. Address G069-G078, G086, G104, G106-G108, and G120: EWS common types/IDs, folders, items, attachments, sync, rules, tasks, extended properties, ConvertId, ResolveNames, search, retention tags, user configuration, and simple schema enums. Use prompts 1-7 in docs/architecture/ews-p0-p1-fix-prompts.md for detailed overlapping work and MS-OXWSCDATA, MS-OXWSITEMID, MS-OXWSFOLD, MS-OXWSMSG, MS-OXWSATT, MS-OXWSSYNC, MS-OXWSCVTID, and MS-OXWSRSLNM.

Acceptance: catalog gate, SOAP behavior tests, and cross-protocol tests prove the adopted bounded canonical behavior. Update the EWS matrix/contract only when behavior changes. Do not add an EWS-local mailbox or property store.

## 7. P1 EWS Canonical Service Families

Apply the shared instructions. Address G033-G034, G036, G075, G087-G092, G094-G102, G104, and G109: availability, service configuration, MailTips, OOF, rules, delegates, DLs, compliance diagnostics, rooms, time zones, sharing, calendaring, tracking, notifications, password expiration, personas/photos, posts/name resolution, and UM. Group changes by canonical owner.

Stay inside the EWS operation contract: rules/OOF map to Sieve; delegation and free/busy map to canonical grants/calendar state; tracking is an LPE/LPE-CT trace projection; directory/compliance data is tenant scoped and Bcc-safe. Test each adopted SOAP family for authorization, invalid input, atomicity, restart replay, and source-of-truth boundaries. Do not create Exchange-only service stores; update EWS/security/data-lifecycle docs as each boundary changes.

## 8. P1 Trace Gate: EWS Push Notifications

Apply the shared instructions. Address needs-trace G103. Pull and bounded streaming replay canonical mail_change_log cursors. Obtain a real client trace proving they are insufficient and write a threat model for callback validation, SSRF, authentication, retry/backoff, affinity, replay, rate limits, and abuse. Use MS-OXWSPSNTIF.

If approved, design push only as a delivery projection over canonical changes. Acceptance before endpoint publication: reviewed trace/threat model plus deterministic retry/expiry/replay, tenant isolation, callback security, and restart tests. Otherwise retain the current capability boundary.

## 9. P0 ActiveSync Mobile Interoperability

Apply the shared instructions. Address G001-G009: ActiveSync commands, WBXML, status, bodies, attachments, scalar types, mail, contacts, calendars, and provisioning. Work in lpe-activesync and canonical owners. Keep scope at 16.1, ItemOperations Fetch attachments, and permissive provisioning. Follow MS-ASHTTP 2.2.1.1.1.1.2, MS-ASWBXML 2.1.2.1, MS-ASCMD 2.2.1, MS-ASAIRS 2.2, MS-ASDTYPE 2.3, and MS-ASPROV 3.

Test exact WBXML/status values, sync tokens, SendMail canonical Sent visibility, attachments, contact/calendar mutation, provisioning, and malformed/foreign input. Run mobile preflight plus Outlook mobile and iOS Mail evidence. Do not add task folders, SMS, Notes, DocumentLibrary, conversation, or IRM breadth.

## 10. P0 Autodiscover Publication And Outlook Shapes

Apply the shared instructions. Address G110-G111. Inspect Autodiscover, LPE-CT edge publication, EWS/MAPI routing, and readiness checks; use MS-OXDISCO and MS-OXDSCLI. Publish only implemented, authenticated LPE-CT-exposed endpoints; do not infer MAPI usability from X-MapiHttpCapability. EXCH, MAPI, and EXPR metadata each require a real transport and its separate gate.

Acceptance: deterministic response/edge tests, scripted readiness, Microsoft RCA, and a real Outlook profile agree before MAPI publication. No unimplemented SMTP or RPC path is advertised. Update autoconfiguration and edge docs.

## 11. P1 IMAP NTLM And Delegate Extension Boundary

Apply the shared instructions. Address keep-explicitly-unsupported G113. Verify IMAP does not advertise NTLM or Exchange delegate extensions and that capability/auth transcripts retain canonical mailbox/rights/Sent behavior. Use MS-OXIMAP4. Do not implement either extension without a blocking Outlook/IMAP trace and an approved auth/delegation architecture.

Acceptance: negative capability and negotiation tests prevent accidental advertisement or widened rights. If a trace satisfies the gate, deliver a minimal proposal; otherwise report the retained boundary and update imap-mvp only when scope changes.

## 12. P0 LPE-CT SMTP AUTH Submission Boundary

Apply the shared instructions. Address G115. Authenticated client SMTP is LPE-CT submission; public ingress AUTH remains unavailable. Inspect LPE-CT submission, core canonical submission, routing, and Autodiscover. Use MS-OXSMTP and MS-XLOGIN 2.2.

Acceptance: submission tests cover authentication, rejection, tenant isolation, handoff/retry/trace, and authoritative LPE Sent visibility; edge tests prove public ingress offers no AUTH; Autodiscover publishes SMTP only for the real authenticated service. Never move SMTP into core or advertise internal relay.

## 13. P0 Spam And Phishing Metadata Boundary

Apply the shared instructions. Address G029. Inspect LPE-CT filtering, reputation, quarantine, traceability, and safe LPE mailbox projection. Use MS-OXCSPAM 2.2 and MS-OXPHISH 2.2. Perimeter decisions remain LPE-CT-owned; LPE may project only safe documented mailbox facts.

Acceptance: trace evidence links each safe projection to its LPE-CT source; tenant, Bcc-safe search/AI, quarantine isolation, stale-metadata, and client-projection tests pass. No public protocol mutates reputation/quarantine or duplicates filtering. Update security/traceability docs.

## 14. P1 Trace Gate: Protected Delegate/Free-Busy Semantics

Apply the shared instructions. Address G067-G068 in the delegation/security context. Obtain Outlook scheduling/delegate traces and permission-bound visibility tests before widening public-folder free/busy or delegate information. Use MS-OXOPFFB and MS-OXODLGT.

Map a proven minimal behavior only to calendar grants, sender rights, delegate_preferences, and calendar events. Acceptance: exact client fields are documented; owner/delegate/non-delegate/private/revocation/cross-protocol tests pass; MAPI identities never leak. Otherwise retain the evidence gate.

## 15. P0 NSPI And EWS Address-Book Projection

Apply the shared instructions. Address G014, G017-G018, G104, and G119: address-book referral, object projection, UI templates, EWS ResolveNames, and NSPI request/property gaps. Inspect NSPI/EWS name resolution and canonical accounts, contacts, groups, and visibility. Use MS-OXABREF, MS-OXOABK 2.2, MS-OXOABKT templates, MS-OXNSPI 3.1.4, and MS-OXPROPS.

Acceptance: NSPI bootstrap, ResolveNames, request ordering/columns, referral/templates, tenant/hidden/ambiguous/missing cases, and property-shape tests pass; Outlook profile evidence confirms resolution; EWS/NSPI visibility agrees without private-contact leakage. Update NSPI and EWS/MAPI architecture for new fields.

## Coverage Checklist

| Prompt | Backlog entry | Decision |
| --- | --- | --- |
| 1 | P0 G013, G043-G051, G116-G118 | implement-now |
| 2 | P0 G027, G055-G064 | implement-now |
| 3 | P1 G052-G054 | needs-trace |
| 4 | P1 G065-G068 | needs-trace |
| 5 | P1 G112 | needs-trace |
| 6 | P0 G069-G078, G086, G104, G106-G108, G120 | implement-now |
| 7 | P1 G033-G034, G036, G075, G087-G092, G094-G102, G104, G109 | implement-now |
| 8 | P1 G103 | needs-trace |
| 9 | P0 G001-G009 | implement-now |
| 10 | P0 G110-G111 | implement-now |
| 11 | P1 G113 | keep-explicitly-unsupported |
| 12 | P0 G115 | implement-now |
| 13 | P0 G029 | implement-now |
| 14 | P1 G067-G068 | needs-trace |
| 15 | P0 G014, G017-G018, G104, G119 | implement-now |

