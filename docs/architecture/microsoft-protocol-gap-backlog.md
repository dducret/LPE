# Microsoft Protocol Gap Backlog

This backlog classifies every `Explicit gaps` row from
`docs/architecture/microsoft-protocol-constants-gap.md`.

It is documentation-only. It does not enable endpoint publication, add protocol
surface area, or change runtime behavior.

## Assumptions

- `Protocol row(s)` uses generated row IDs from the current constants gap report
  table, in table order: `G001` is the first data row, `G120` is the last.
- One backlog entry may cover multiple generated rows when the same product
  decision, evidence gate, and implementation owner apply.
- Priorities are backlog planning priorities, not bug severities:
  - `P0`: blocks current advertised or gated Outlook/mobile/native-client goals.
  - `P1`: high-value compatibility work after current gates or when traces prove
    it is required.
  - `P2`: bounded parity or canonical-model work, not a current gate.
  - `P3`: documentation/audit clarity or explicitly unsupported breadth.
  - `P4`: non-product or deferred Exchange ecosystem breadth.

## Audit Result

Status as of 2026-07-09:

| Check | Result |
| --- | --- |
| Generated report freshness | Passed: `cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current` |
| Report rows classified in this backlog | Passed: 120 of 120 generated rows are referenced after expanding row ranges |
| Duplicate protocol IDs in `docs/microsoft/protocol-sources.toml` | Passed: none found |
| Cached PDF has a protocol-source entry | Passed: `MS-OXCFXICS`, `MS-OXCMAPIHTTP`, and `MS-OXNSPI` source entries point to the cached `docs/microsoft/exchange_protocols/` PDFs used by the audit |
| Cached protocol ID appears in the generated constants gap report | Passed: protocol entries map to generated report rows; the Exchange documentation-set readme is recorded as a non-protocol `[[reference]]` and is intentionally outside the constants gap report |

Final row status counts for the generated report:

| Status | Rows | Count |
| --- | --- | ---: |
| Implemented and tested / bounded by concrete code or protocol tests | G001-G009, G013-G014, G017-G018, G022, G027, G029, G033-G034, G036, G043-G064, G069-G078, G086-G092, G094-G097, G099-G104, G106-G113, G115-G120 | 82 |
| Intentionally unsupported and tested/documented | G010-G012, G015-G016, G019-G021, G025-G026, G030-G031, G035, G037-G042, G080, G093, G098, G105, G114 | 24 |
| Deferred with clear architecture rationale / trace gate | G023-G024, G028, G032, G065-G068, G079, G081-G085 | 14 |
| Still untriaged or weakly evidenced | none at row level | 0 |

The row-level audit is classified, and the protocol-source coverage blockers are
resolved for this revision.

## Outlook Profile/MAPI

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G013, G043-G051, G116-G118 | MAPI/HTTP request, ROP, table, folder, special-folder, permission, notification, store-logon, property-type, FastTransfer/ICS, and Outlook configuration gaps that affect profile bootstrap, cached-mode sync, send, reconnect, and view/FAI behavior. | implement-now | MAPI over HTTP is the primary Outlook desktop route. Existing scope is bounded to authenticated EMSMDB/NSPI over canonical state, with unsupported values returning parseable errors. Cite MS-OXCMAPIHTTP 2.2.3.3.1, 2.2.4; MS-OXCROPS 2.2; MS-OXCDATA 2.8; MS-OXCFXICS 2.2.4; MS-OXOCFG 4.1-4.4. | Local `lpe-exchange` tests, `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness`, Microsoft RCA, and separate Outlook 2016/2019 cached-mode evidence. | `lpe-exchange` MAPI/HTTP, ROP, sync, tables, properties. | `docs/architecture/mapi-over-http-implementation-plan.md`; `docs/architecture/outlook-cached-mode-gate-evidence-template.md`; focused `cargo test -p lpe-exchange mapi_over_http` and protocol golden tests. |
| P0 | G027, G055-G064 | Named-property mapping plus Outlook mail, flag, rules, calendar, contact, task, note, journal, reminder, and post object properties. | implement-now | Outlook cached mode needs stable named-property IDs and bounded object projections, but canonical LPE state remains authoritative. Do not store opaque Exchange blobs as active behavior unless a canonical model exists. Cite MS-OXCPRPT 3.1.4.1; MS-OXOMSG 2.2; MS-OXOFLAG 2.2; MS-OXOCAL 2.2; MS-OXOCNTC 2.2; MS-OXOTASK 2.2. | Outlook traces showing property sets used during profile, sync, compose, calendar, contacts, tasks, and rules; cross-protocol JMAP/IMAP/EWS visibility checks. | `lpe-exchange` MAPI properties/object mapping plus canonical storage owners. | MAPI property/object tests, `docs/architecture/mapi-full-object-support-execution.md`, relevant canonical object docs when fields are added. |
| P1 | G052-G054 | Best-body, MIME generation, and iCalendar conversion gaps. | needs-trace | Body/calendar conversion should widen only for observed Outlook/EWS/client failures. Current behavior uses canonical message bodies, sanitized HTML, MIME blobs, and bounded calendar fields. Cite MS-OXBBODY 2.1; MS-OXCMAIL 2.1; MS-OXCICAL 2.1. | Outlook/EWS/ActiveSync traces proving a missing body or calendar conversion shape blocks real workflows. | Message conversion, calendar, and attachment owners. | Conversion golden tests; `docs/architecture/ews-mapi-mvp.md`; `docs/architecture/activesync-mvp.md` if mobile behavior changes. |
| P1 | G065-G068 | RSS, document message objects, public-folder free/busy, and delegate information object gaps. | needs-trace | These are Outlook object-family parity areas, not first MAPI publication requirements unless traces show Outlook setup or cached mode depends on them. Delegate/free-busy must map to canonical grants and availability. Cite MS-OXORSS 2.2; MS-OXODOC 2.2; MS-OXOPFFB 2.2; MS-OXODLGT 2.2. | Real Outlook traces for RSS/document/delegate/free-busy flows; scheduling/delegate lab evidence for any implementation. | MAPI object mapping, delegation/free-busy owners. | `docs/architecture/mapi-full-object-support-execution.md`; delegate/free-busy tests; Outlook trace fixtures. |
| P1 | G112 | Outlook Anywhere RPC proxy remains a legacy shim behind separate EXPR gates. | needs-trace | MAPI over HTTP is first. RPC/HTTP must not be publicly advertised until `/rpc/rpcproxy.dll` behavior matches authenticated mailbox transport expectations. Cite MS-OXCRPC EMSMDB interface sections used by the RPC proxy tests. | Microsoft RCA Outlook Anywhere evidence and real legacy Outlook profile traces for the same host before EXPR publication. | `lpe-exchange` RPC proxy plus LPE-CT edge routing. | RPC proxy tests; `docs/architecture/client-autoconfiguration.md`; edge install checks. |

## EWS

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G069-G078, G086, G104, G106-G108, G120 | EWS common types, identifiers, folders, messages/items, attachments, synchronization, inbox rules, tasks, extended properties, core operations, ConvertId, ResolveNames, search, retention tags, user configuration, and simple schema enum gaps. | implement-now | EWS is an active Exchange compatibility adapter over canonical state. These rows cover the core EWS/native-client surface and must stay parseable, bounded, and Bcc-safe. Cite MS-OXWSCDATA common types; MS-OXWSITEMID identifiers; MS-OXWSFOLD/MS-OXWSMSG/MS-OXWSATT operations; MS-OXWSSYNC sync; MS-OXWSCVTID ConvertId; MS-OXWSRSLNM ResolveNames. | EWS catalog gate, SOAP operation tests, cross-protocol state checks, and client traces for any widened behavior. | `lpe-exchange` EWS plus canonical mailbox/contact/calendar/task/storage owners. | `docs/architecture/ews-interoperability-matrix.md`; `cargo test -p lpe-exchange ews`; SOAP behavior tests. |
| P1 | G033-G034, G036, G075, G087-G092, G094-G102, G104, G109 | Availability/free-busy, service configuration, MailTips, OOF, inbox rules, delegate, DL, eDiscovery, non-indexable diagnostics, rooms, time zones, sharing, calendaring, message tracking, notifications, password expiration, personas/photos/posts/name resolution, and UM gaps. | implement-now | These are current bounded EWS families when they map to canonical LPE or LPE-CT state; implementation must not create Exchange-only stores. Message tracking remains an LPE-CT trace projection. Cite MS-OXWAVLS, MS-OXWCONFIG, MS-OXWMT, MS-OXWOOF, MS-OXWSDLGM, MS-OXWSNTIF, MS-OXWSMTRK, MS-OXWUMS sections recorded in `protocol-sources.toml`. | SOAP tests per operation family; tenant-boundary, permission, Bcc, traceability, and restart/replay evidence. | EWS operation owners, canonical delegation/calendar/rules/compliance, LPE-CT traceability. | `docs/architecture/ews-interoperability-matrix.md`; `docs/architecture/mail-security-and-traceability.md`; data lifecycle docs for compliance/search changes. |
| P2 | G079-G085 | EWS SOAP Autodiscover, archive, bulk transfer, client extensions, contacts/photos, conversations, Unified Contact Store, and mail app gaps. | needs-trace | Useful Exchange ecosystem breadth, but not a first Outlook MAPI profile gate. Widen only when a target client/add-in/compliance workflow requires it and a canonical model exists. Cite the corresponding MS-OXWS* operation sections. | Client/add-in/admin-tool traces proving the missing operation or payload is needed. | EWS feature-family owners. | EWS operation tests and architecture docs for each adopted canonical model. |
| P3 | G080, G093, G098, G105 | EWS archive breadth, federated Internet authentication, online personal search, and site mailbox operations. | keep-explicitly-unsupported | Archive semantics are bounded; Live ID federation, online personal search, and SharePoint site mailbox coupling are outside current LPE product boundaries. Cite MS-OXWSARCH, MS-OXWSLVID, MS-OXWSOLPS, MS-OXWSSMBX. | Product decision changing scope plus architecture update before implementation. | Product architecture, EWS owner if scope changes. | `docs/architecture/ews-interoperability-matrix.md`; unsupported SOAP tests. |
| P1 | G103 | EWS push notification callback handling is absent. | needs-trace | LPE currently uses pull and bounded streaming over canonical change-log cursors. Push requires endpoint validation, retries, affinity, and abuse controls before implementation. Cite MS-OXWSPSNTIF. | Real EWS client evidence requiring push rather than pull/streaming; threat model for callback endpoints. | EWS notifications and security owners. | Notification tests, threat model, `docs/architecture/ews-mapi-mvp.md`. |

## ActiveSync

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G001-G009 | ActiveSync command, WBXML, status, body/attachment, scalar data type, mail, contact, calendar, and provisioning gaps in the mobile interoperability surface. | implement-now | ActiveSync is the flagship mobile/native-client path. Keep support bounded to version 16.1, canonical state, `ItemOperations Fetch` for attachments, and permissive provisioning by default. Cite MS-ASHTTP 2.2.1.1.1.1.2; MS-ASWBXML 2.1.2.1; MS-ASCMD 2.2.1; MS-ASAIRS 2.2; MS-ASDTYPE 2.3; MS-ASPROV 3. | `cargo test -p lpe-activesync`, mobile preflight, Outlook mobile and iOS Mail lab evidence. | `lpe-activesync` plus canonical mailbox/contact/calendar/attachment owners. | `docs/architecture/activesync-mvp.md`; `docs/architecture/activesync-interoperability-matrix.md`; WBXML/status tests. |
| P2 | G010 | ActiveSync task-folder sync exists only as Email follow-up flag token support; SMS, Notes, and DocumentLibrary classes are unsupported. | keep-explicitly-unsupported | Canonical tasks exist, but ActiveSync task-folder sync is deferred; SMS, Notes, and DocumentLibrary are protocol breadth outside the current mobile gate. Cite MS-ASTASK 2.2; MS-ASMS 2.2; MS-ASNOTE 2.2; MS-ASDOC 2.2. | Product decision to expose task folders through ActiveSync plus mobile client traces. | ActiveSync and tasks owners. | ActiveSync docs/tests if task sync becomes in scope; unsupported code-page tests otherwise. |
| P2 | G011-G012 | ActiveSync conversation and rights-management namespaces are not implemented. | keep-explicitly-unsupported | Conversation operations and IRM require canonical conversation/rights models and protected content handling that are not current ActiveSync scope. Cite MS-ASCON 2.2.2; MS-ASRM 2.2.2. | Real mobile-client trace showing required behavior plus canonical rights/conversation architecture. | ActiveSync, security, and mailbox conversation owners. | ActiveSync docs, security docs, and WBXML unsupported-manifest tests. |

## DAV/IMAP/SMTP

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G110-G111 | Autodiscover publication and Outlook response-shape gaps. | implement-now | Endpoint publication is a gate, not feature breadth. Autodiscover must publish only implemented and exposed endpoints through LPE-CT and must not infer MAPI from `X-MapiHttpCapability` alone. Cite MS-OXDISCO and MS-OXDSCLI publication/client sections. | Edge checks, scripted readiness, Microsoft RCA, real Outlook profile evidence before MAPI/EXPR publication. | Autoconfiguration, LPE-CT edge, EWS/MAPI owners. | `docs/architecture/client-autoconfiguration.md`; edge install/test scripts; autodiscover tests. |
| P1 | G113 | Exchange IMAP extension gaps: NTLM and delegate extensions not implemented. | keep-explicitly-unsupported | IMAP remains a mailbox compatibility layer over canonical state; no IMAP-local rights or Sent state. NTLM/delegate extensions are not advertised. Cite MS-OXIMAP4 extension sections. | Outlook/IMAP client trace proving a missing extension blocks supported IMAP setup. | `lpe-imap` and auth/delegation owners if scope changes. | `docs/architecture/imap-mvp.md`; IMAP capability and transcript tests. |
| P0 | G115 | SMTP AUTH behavior is bounded to LPE-CT authenticated submission; public ingress AUTH is intentionally unavailable. | implement-now | SMTP belongs to LPE-CT. Core LPE must not expose client SMTP, and autodiscover may publish SMTP only when real authenticated submission is deployed. Cite MS-OXSMTP and MS-XLOGIN 2.2. | LPE-CT submission tests, edge publication checks, canonical Sent/submission trace evidence. | LPE-CT SMTP/submission plus core submission owners. | `docs/architecture/client-autoconfiguration.md`; `docs/architecture/mail-security-and-traceability.md`; LPE-CT SMTP tests. |
| P2 | G039-G040 | SMTP AUTH LOGIN is bounded; Exchange OAuth extension breadth is incomplete. | needs-trace | LOGIN/PLAIN support is implemented where deployed; OAuth extension breadth must align with mailbox auth architecture and not become an SMTP-only auth model. Cite MS-XLOGIN 2.2 and MS-XOAUTH 2.2-3.x. | Client traces requiring XOAUTH shape beyond current auth tokens. | Auth, LPE-CT submission, autoconfiguration owners. | Auth docs, submission tests, autoconfig tests. |
| P3 | G041-G042 | Legacy Exchange WebDAV calendar and security descriptor extensions are not implemented. | keep-explicitly-unsupported | LPE supports CardDAV/CalDAV/VTODO over canonical contacts/calendars/tasks, not legacy Exchange WebDAV ACL/security-descriptor semantics. Cite MS-XWDCAL 2.2 and MS-XWDVSEC 2.2. | Product decision to support legacy Exchange WebDAV clients. | DAV and rights owners if scope changes. | `docs/architecture/dav-mvp.md`; DAV unsupported-property tests. |
| P4 | G114 | POP3 Exchange extensions are not implemented or advertised. | keep-explicitly-unsupported | POP3 is outside current LPE client protocol scope. | Product decision adding POP3 to architecture. | Product architecture. | `ARCHITECTURE.md`; client-autoconfiguration docs if scope changes. |

## Security/Protected Content

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G029 | Spam/phishing metadata gaps. | implement-now | Spam, phishing, reputation, quarantine, and transport security belong to LPE-CT; LPE may project safe canonical mailbox facts only. Cite MS-OXCSPAM 2.2 and MS-OXPHISH 2.2. | LPE-CT filtering/trace evidence; Bcc-safe user search/AI tests. | LPE-CT security and core mailbox projection owners. | `docs/architecture/mail-security-and-traceability.md`; security tests; traceability docs. |
| P2 | G030 | Rights-managed email and S/MIME conversion gaps. | needs-trace | Protected content cannot be parsed, decrypted, stripped, or indexed without a canonical security model. Rights-management breadth is not a current interop gate. Cite MS-OXORMMS 2.2 and MS-OXOSMIME 2.1. | Real client traces plus a security architecture for keys, decryption, indexing, and policy. | Security, message conversion, ActiveSync/EWS/MAPI owners. | Threat model, data lifecycle docs, conversion tests. |
| P2 | G037-G038 | Journal report file format and Exchange postmark validation gaps. | keep-explicitly-unsupported | Transport journaling/postmark validation are Exchange ecosystem features outside the current canonical mailbox and LPE-CT security scope. Cite MS-XJRNL 2.1 and MS-OXPSVAL 2.1. | Compliance or anti-spam product decision changing scope. | Product/security architecture. | Mail security docs and LPE-CT docs if adopted. |
| P1 | G067-G068 | Public-folder free/busy and delegate information object gaps where they affect protected availability/delegation semantics. | needs-trace | Free/busy and delegate data must derive from canonical grants, sender rights, and calendar events without exposing private details. Cite MS-OXOPFFB and MS-OXODLGT sections recorded in `protocol-sources.toml`. | Outlook scheduling/delegate traces and permission-bound visibility tests. | Delegation/free-busy and MAPI owners. | Delegation/free-busy docs/tests. |

## Message Formats/Conversion

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P2 | G020-G021 | Full `.msg` import/export and TNEF parsing/generation are absent. | needs-trace | Current MAPI paths use canonical message/attachment state. Full `.msg` and TNEF round-tripping is risky breadth unless real Outlook workflows require it. Cite MS-OXMSG and MS-OXTNEF processing sections. | Outlook/client traces requiring `.msg` or `winmail.dat` fidelity; conversion golden samples. | Message conversion and attachment owners. | `docs/architecture/mapi-full-object-support-execution.md`; conversion tests; Magika validation path. |
| P2 | G022 | Full vCard conversion parity is absent. | implement-now | DAV/JMAP contacts already own bounded vCard behavior over canonical contacts. Widen only when canonical contact fields exist and tests prove lossless behavior for supported fields. Cite MS-OXVCARD 2.1.3. | DAV/JMAP contact round-trip fixtures for adopted fields. | DAV/JMAP contacts and canonical contact storage owners. | `docs/architecture/dav-mvp.md`; `docs/architecture/jmap-contacts-calendars-mvp.md`; vCard tests. |
| P2 | G023-G024 | RTF compression and RTF encapsulation are not fully implemented. | needs-trace | LPE can synthesize simple RTF projections, but full LZFu/RTF encapsulation should wait for Outlook traces proving rich-body breakage. Cite MS-OXRTFCP 2.1-2.2 and MS-OXRTFEX 2.1-2.2. | Outlook compose/read traces with compressed RTF dependency; golden decompression/encapsulation fixtures. | MAPI body/property conversion owners. | MAPI body tests; conversion architecture notes if adopted. |
| P3 | G025 | Microsoft compression structures for MSZIP/LZX DELTA are not implemented. | keep-explicitly-unsupported | Compression package support is not a current LPE protocol surface. Cite MS-MCI applicability and MS-PATCH 2.1-2.2. | Specific protocol payload requiring MSZIP/LZX DELTA in current product scope. | Protocol owner for the requiring surface. | Gap report/audit docs only unless scope changes. |
| P3 | G035 | Offline Address Book web retrieval package formats are not implemented. | keep-explicitly-unsupported | LPE uses NSPI/EWS/canonical directory projections and does not publish OAB web packages. Cite MS-OXWOAB 2.1-2.2. | Product decision to support OAB downloads plus Outlook trace need. | Address book/OAB owner. | NSPI/OAB docs and OAB package tests if adopted. |

## Address Book/OAB

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P0 | G014, G017-G018, G104, G119 | Address-book referral, object projection, UI templates, EWS name resolution, and NSPI request/property gaps. | implement-now | NSPI is required for Outlook profile creation and recipient resolution. It must project canonical accounts, contacts, groups, and visibility, not mutate directory state. Cite MS-OXABREF, MS-OXOABK 2.2, MS-OXOABKT template sections, MS-OXNSPI 3.1.4, MS-OXPROPS address-book properties. | NSPI bootstrap tests, Outlook profile evidence, tenant/hidden-entry tests, EWS ResolveNames tests. | `lpe-exchange` NSPI/EWS name-resolution owners. | `docs/architecture/nspi-support-matrix.md`; `docs/architecture/ews-mapi-mvp.md`; NSPI tests. |
| P2 | G016, G019, G035 | Offline Address Book files, public-folder OAB retrieval, and OAB web retrieval are absent. | keep-explicitly-unsupported | LPE does not generate OAB v2/v3/v4 files or distribute OAB through public folders/web packages. Use NSPI/EWS/canonical directory until a real client requirement appears. Cite MS-OXOAB, MS-OXPFOAB, MS-OXWOAB. | Outlook traces proving OAB download is required for a supported mode. | Address book/OAB owner. | OAB architecture docs and package tests if scope changes. |
| P4 | G015 | Exchange LDAP extensions and AD service connection points are absent. | keep-explicitly-unsupported | LPE is not an Active Directory or Exchange LDAP server; discovery/address book use Autodiscover, EWS, NSPI, and canonical APIs. Cite MS-OXLDAP. | Product decision to integrate with AD/LDAP as a first-class feature. | Identity/directory architecture. | `docs/architecture/client-autoconfiguration.md`; identity docs if adopted. |

## Deferred/Non-Product Protocols

| Priority | Protocol row(s) | Gap summary | Decision | Rationale | Required evidence | Proposed implementation owner | Tests/docs to update |
| --- | --- | --- | --- | --- | --- | --- | --- |
| P3 | G026 | Glossary/reference/system overview rows need only audit anchors. | documentation-only | These rows classify terminology and protocol-family boundaries, not implementable behavior. Cite MS-OXGLOS, MS-OXREF, MS-OXPROTO. | None beyond keeping source anchors current. | Architecture/docs owner. | `docs/microsoft/protocol-sources.toml`; generated audit report. |
| P3 | G028 | Client extension message objects are bounded to current EWS mail-app catalog behavior. | needs-trace | Add-in/app metadata should use canonical catalog/install/token state, not opaque Exchange extension messages, unless real add-in clients require it. Cite MS-OXCEXT 2.1-2.2. | Outlook add-in traces and EWS app operation evidence. | EWS mail-app owner. | EWS mail app tests and docs. |
| P4 | G031 | SMS/MMS, voicemail, and fax object formats are not product scope beyond bounded EWS UM call-state compatibility. | keep-explicitly-unsupported | These are legacy/adjacent Exchange object families, not current LPE mailbox/collaboration state. Cite MS-OXOSMMS and MS-OXOUM. | Product decision adding SMS/MMS/voicemail/fax as canonical objects. | Product architecture. | EWS/UM docs if scope changes. |
| P3 | G032 | Sharing messages and sharing attachment schema are not full Exchange message-object parity. | needs-trace | Sharing should map to canonical grants/invitations; Exchange sharing message blobs must not become active grant truth by default. Cite MS-OXSHARE 2.2 and MS-OXSHRMSG 2.1. | Outlook/EWS sharing invitation traces requiring the message/attachment schema. | Sharing/EWS owners. | Sharing docs/tests; EWS sharing tests. |
| P4 | G093, G098, G105, G114 | Live ID federation, online personal search, site mailboxes, and POP3 remain outside product scope. | keep-explicitly-unsupported | These features contradict or exceed current architecture priorities and should remain parseable unsupported/no-publication surfaces. | Product decision and architecture update. | Product architecture. | Relevant architecture docs only if scope changes. |

## Coverage Check

Run this PowerShell one-liner from the repository root after editing this file:

```powershell
$sourceRows = (Get-Content docs/architecture/microsoft-protocol-constants-gap.md | Where-Object { $_ -match '^\| ' -and $_ -notmatch '^\| ---' -and $_ -notmatch '^\| Surface ' }).Count; $refs = [System.Collections.Generic.SortedSet[int]]::new(); foreach ($m in [regex]::Matches((Get-Content -Raw docs/architecture/microsoft-protocol-gap-backlog.md), 'G(\d{3})(?:-G(\d{3}))?')) { $start = [int]$m.Groups[1].Value; $end = if ($m.Groups[2].Success) { [int]$m.Groups[2].Value } else { $start }; $start..$end | ForEach-Object { [void]$refs.Add($_) } }; $missing = 1..$sourceRows | Where-Object { -not $refs.Contains($_) }; [pscustomobject]@{SourceRows=$sourceRows;BacklogRefs=$refs.Count;Missing=($missing -join ',')}
```

Expected result for this revision:

| SourceRows | BacklogRefs | Missing |
| --- | --- | --- |
| 120 | 120 |  |
