# Microsoft Protocol Gap Burn-Down Prompts

Use these prompts to drive follow-up Codex runs from
`docs/architecture/microsoft-protocol-constants-gap.md`.

The gap report is an audit boundary, not a mandate to implement every Microsoft
Exchange feature. Each prompt must preserve LPE architecture: canonical mailbox,
contacts, calendar, task, rights, search, and submission state stay in core
`LPE`; perimeter SMTP, filtering, quarantine, and relay behavior stay in
`LPE-CT`; client Autodiscover must publish only endpoints that are actually
implemented and exposed.

## Prompt 1: Build The Complete Gap Backlog

```text
You are working in C:\Development\LPE.

Goal: turn every "Explicit gaps" cell in docs/architecture/microsoft-protocol-constants-gap.md into a concrete, prioritized backlog entry with an implementation decision.

Required context before edits:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/microsoft/protocol-sources.toml only for protocols you touch.
- Read the architecture document directly relevant to each protocol family you classify.

Rules:
- Do not implement features in this pass unless the smallest correct action is an audit/test/doc correction.
- Do not treat all gaps as bugs. Classify each as one of:
  - implement-now: needed for current Outlook/EWS/MAPI/ActiveSync/DAV/SMTP interoperability goals
  - keep-explicitly-unsupported: correct architectural boundary or deferred protocol breadth
  - documentation-only: audit row needs clearer wording, source sections, or stronger anchors
  - needs-trace: requires real Outlook/RCA/client evidence before implementation
- Do not copy Microsoft spec text. Cite protocol IDs and sections.
- Keep source under Apache-2.0 and do not add dependencies without LICENSE.md review.

Deliverable:
- Create docs/architecture/microsoft-protocol-gap-backlog.md.
- Include one row per gap cluster, not necessarily one row per sentence.
- Columns: Priority, Protocol row(s), Gap summary, Decision, Rationale, Required evidence, Proposed implementation owner, Tests/docs to update.
- Group by: Outlook profile/MAPI, EWS, ActiveSync, DAV/IMAP/SMTP, security/protected content, message formats/conversion, address book/OAB, deferred/non-product protocols.

Verification:
- Confirm every row in microsoft-protocol-constants-gap.md is represented by at least one backlog entry.
- Add a small script or documented PowerShell one-liner that counts report rows and backlog references.
- Do not mark complete until the row count and references prove full coverage.
```

## Prompt 2: Outlook Desktop MAPI And NSPI Depth

```text
You are working in C:\Development\LPE.

Goal: address the high-priority Outlook desktop gaps in docs/architecture/microsoft-protocol-constants-gap.md for MAPI over HTTP, EMSMDB, NSPI, FastTransfer/ICS, ROPs, OAB/address-book behavior, folder/message/contact/calendar/task projections, rules, reminders, search folders, configuration FAI, and Outlook Anywhere/RPC proxy compatibility.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/ews-mapi-mvp.md, docs/architecture/mapi-over-http-implementation-plan.md, docs/architecture/outlook-exchange-parity-roadmap.md, docs/architecture/mapi-full-object-support-execution.md, docs/architecture/nspi-support-matrix.md, and docs/architecture/outlook-cached-mode-gate-evidence-template.md.
- Check official Microsoft Learn Open Specifications for the exact protocol sections before changing code.

Scope:
- Focus only on gaps that can block Outlook 2016/2019/Microsoft 365 Apps profile creation, cached-mode sync, folder hierarchy, associated contents, address book resolution, send/draft/Sent behavior, reconnect, shutdown, and view startup.
- Candidate rows include MS-OXCMAPIHTTP, MS-OXCRPC, MS-OXCROPS, MS-OXCDATA, MS-OXCFXICS, MS-OXCMSG, MS-OXCTABL, MS-OXCFOLD, MS-OXCSTOR, MS-OXABREF, MS-OXOABK, MS-OXOABKT, MS-OXOSFLD, MS-OXOSRCH, MS-OXOCFG, MS-OXCPERM, MS-OXCNOTIF, MS-OXORULE, MS-OXORMDR, MS-OXOMSG, MS-OXOCAL, MS-OXOCNTC, MS-OXOTASK, MS-OXOPOST, MS-OXOPFFB, MS-OXOAB, MS-OXPFOAB, MS-OXWOAB, and NSPI request/property rows.

Work method:
- For each candidate gap, decide whether Outlook needs it for the next profile/cached-mode gate.
- Implement only the smallest protocol-correct behavior that maps to canonical LPE state.
- Reject or keep explicit gaps for Exchange-only stores, opaque blobs, provider-specific state, or behavior that would create parallel Sent/Outbox/mailbox truth.
- Add focused tests before or with code changes. Use realistic protocol builders and existing MAPI/ROP helpers.
- Update microsoft_protocol_audit.rs and regenerate microsoft-protocol-constants-gap.md when a gap changes status.

Verification:
- Run focused cargo tests for every touched protocol path.
- Run cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current.
- If behavior affects Outlook publication readiness, update the Outlook evidence docs instead of enabling public MAPI Autodiscover prematurely.
```

## Prompt 3: EWS Collaboration, Availability, OOF, MailTips, Notifications, And Sharing

```text
You are working in C:\Development\LPE.

Goal: address EWS gaps in docs/architecture/microsoft-protocol-constants-gap.md while preserving canonical LPE collaboration state.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/ews-interoperability-matrix.md, docs/architecture/ews-mapi-mvp.md, docs/architecture/collaboration-acl-delegation-mvp.md, docs/architecture/data-lifecycle-and-compliance.md, docs/architecture/public-folders-mapi-mvp.md, docs/architecture/notes-journal-reminders.md, and docs/architecture/tasks-mvp.md as relevant.
- Check official Microsoft Learn Open Specifications for each EWS protocol row before edits.

Scope:
- Candidate rows include MS-OXWSCDATA, MS-OXWSCORE, MS-OXWSFOLD, MS-OXWSMSG, MS-OXWSATT, MS-OXWSSYNC, MS-OXWSCONT, MS-OXWSCONV, MS-OXWSCOS, MS-OXWSDLIST, MS-OXWSMSHR, MS-OXWSCAL, MS-OXWAVLS, MS-OXWOOF, MS-OXWCONFIG, MS-OXWMT, MS-OXWSNTIF, MS-OXWSPSNTIF, MS-OXWSRSLNM, MS-OXWSSRCH, MS-OXWSRULES, MS-OXWSTASK, MS-OXWSXPROP, MS-OXWSUSRCFG, MS-OXWSPERS, MS-OXWSPHOTO, MS-OXWSPED, MS-OXWSPST, MS-OXWSEDISC, MS-OXWSGNI, MS-OXWSGTRM, MS-OXWUMS, MS-OXWSPOST, and MS-OXWSSMBX.

Work method:
- Start with gaps that already map cleanly to canonical state: folder/item sync, availability, OOF, MailTips, sharing, delegate/free-busy, public-folder posts, tasks, contacts, calendar, notifications, retention, and user configuration.
- Keep unsupported or parseable-error behavior for Exchange-only stores, federation, site mailboxes, PBX/UM transport, arbitrary transfer packages, protected content, and unsupported service-configuration payloads unless architecture docs explicitly make them in-scope.
- Do not introduce EWS-local canonical stores.
- Add or update EWS tests in crates/lpe-exchange/src/tests/ews.rs and lower-level helpers as needed.

Verification:
- Run focused EWS tests for touched operations.
- Run cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current.
- Confirm Bcc remains protected in any search, discovery, transfer, or item projection changes.
```

## Prompt 4: ActiveSync, DAV, IMAP, SMTP, Autodiscover, And Edge Publication

```text
You are working in C:\Development\LPE.

Goal: address non-MAPI client and edge-publication gaps in docs/architecture/microsoft-protocol-constants-gap.md.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/activesync-mvp.md, docs/architecture/activesync-interoperability-matrix.md, docs/architecture/dav-mvp.md, docs/architecture/imap-mvp.md, docs/architecture/client-autoconfiguration.md, docs/architecture/edge-and-protocol-exposure.md, docs/architecture/lpe-ct-integration.md, and docs/architecture/mail-security-and-traceability.md as relevant.
- Check official Microsoft Learn Open Specifications for each protocol row before edits.

Scope:
- Candidate rows include MS-ASHTTP, MS-ASCMD, MS-ASWBXML, MS-ASAIRS, MS-ASDTYPE, MS-ASEMAIL, MS-ASCNTC, MS-ASCAL, MS-ASPROV, MS-ASTASK, MS-ASCON, MS-ASRM, MS-OXIMAP4, MS-OXPOP3, MS-OXSMTP, MS-XLOGIN, MS-XOAUTH, MS-XWDCAL, MS-XWDVSEC, MS-OXDISCO, MS-OXDSCLI, MS-OXWSADISC, MS-OXLDAP, and SMTP/auth/autodiscover-related rows.

Work method:
- Implement only behavior that is exposed by a real configured endpoint.
- Keep public SMTP AUTH disabled on ingress; authenticated client submission belongs to LPE-CT only.
- Keep POP3 and LDAP unadvertised unless architecture explicitly changes.
- DAV must stay a compatibility layer over canonical contacts/calendar/tasks and grants, not DAV-local file or ACL storage.
- ActiveSync must keep mobile/native scope and must not be used as the Outlook desktop Exchange-account path.
- Autodiscover must publish only real endpoints and must respect MAPI/EXPR gates.

Verification:
- Run focused tests in lpe-activesync, lpe-dav, lpe-imap, lpe-exchange Autodiscover/EWS catalog, and LPE-CT as appropriate.
- Run edge script checks only when the change affects installation or endpoint publication.
- Run cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current if audit rows change.
```

## Prompt 5: Message Formats, Conversion Algorithms, Protected Content, And Attachments

```text
You are working in C:\Development\LPE.

Goal: address message-format and conversion gaps from docs/architecture/microsoft-protocol-constants-gap.md without corrupting canonical LPE message state.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/attachments-v1.md, docs/architecture/ews-mapi-mvp.md, docs/architecture/mapi-full-object-support-execution.md, docs/architecture/mail-security-and-traceability.md, docs/architecture/jmap-mail-mvp.md, and docs/architecture/jmap-contacts-calendars-mvp.md as relevant.
- Check official Microsoft Learn Open Specifications for exact algorithms before edits.

Scope:
- Candidate rows include MS-OXCMAIL, MS-OXBBODY, MS-OXTNEF, MS-OXMSG, MS-OXRTFCP, MS-OXRTFEX, MS-OXVCARD, MS-OXCICAL, MS-OXOSMIME, MS-OXORMMS, MS-MCI, MS-PATCH, MS-OXPSVAL, MS-OXOSMMS, MS-OXOUM, MS-XJRNL, MS-OXODOC, MS-OXORSS, and attachment-related EWS/MAPI rows.

Work method:
- Prefer canonical parsers/serializers already in LPE.
- Every external/client-provided file must pass Magika validation before normal processing.
- Do not add GPL/LGPL/AGPL/SSPL/non-standard dependencies. Review LICENSE.md before adding any parser/compression dependency.
- Preserve Bcc protections in MIME rendering, search, export, AI, and transfer paths.
- For unsupported algorithms, keep parseable errors or explicit audit rows rather than silently accepting opaque data that LPE cannot safely model.
- If implementing import/export, add round-trip tests with deterministic fixtures and realistic protocol builders.

Verification:
- Run focused tests for body selection, MIME rendering, attachment import/export, DAV/JMAP contact/calendar conversion, MAPI stream/property projection, and any compression/parser helpers added.
- Run cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current if audit rows change.
```

## Prompt 6: Security, Compliance, Anti-Abuse, And Perimeter-Owned Protocol Gaps

```text
You are working in C:\Development\LPE.

Goal: address security/compliance/perimeter gaps from docs/architecture/microsoft-protocol-constants-gap.md while preserving the LPE/LPE-CT responsibility split.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/mail-security-and-traceability.md, docs/architecture/lpe-ct-local-data-stores.md, docs/architecture/lpe-ct-integration.md, docs/architecture/data-lifecycle-and-compliance.md, docs/architecture/mailbox-modern-auth-mvp.md, and docs/architecture/client-autoconfiguration.md as relevant.
- Check official Microsoft Learn Open Specifications before edits.

Scope:
- Candidate rows include MS-OXCSPAM, MS-OXPHISH, MS-OXPSVAL, MS-OXORMMS, MS-OXOSMIME, MS-ASRM, MS-OXWSEDISC, MS-OXWSGNI, MS-OXWSURPT, MS-OXWSPED, MS-XOAUTH, MS-XWDVSEC, MS-OXSMTP, MS-XLOGIN, and any row whose gap can expose protected metadata or cross-tenant state.

Work method:
- LPE-CT owns perimeter spam/security scoring, quarantine, DKIM/SPF/DMARC, reputation, and SMTP ingress/relay.
- Core LPE owns canonical mailbox, rights, retention, compliance search, and protected metadata rules.
- Never expose Bcc in user search, AI pipelines, EWS discovery, MIME rendering for normal mailbox access, or diagnostics.
- Do not implement opaque Exchange security blobs as active behavior unless a canonical LPE model exists.
- Add tests that prove rejection/unsupported behavior has no side effects where full support is not implemented.

Verification:
- Run focused LPE-CT tests for perimeter changes and focused lpe-exchange/lpe-storage tests for compliance/retention/discovery changes.
- Run cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current if audit rows change.
```

## Prompt 7: Audit Row Hardening And Unsupported-Boundary Tests

```text
You are working in C:\Development\LPE.

Goal: improve microsoft-protocol-constants-gap.md coverage quality without expanding protocol behavior.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read crates/lpe-exchange/src/microsoft_protocol_audit.rs.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/microsoft/protocol-sources.toml for the rows you touch.

Work method:
- Find report rows where the anchor is too broad, stale, or points only to documentation when a focused test exists.
- Add focused tests that prove explicit unsupported boundaries where that is safer than implementing behavior.
- Improve row wording when it could be misread as full Exchange parity.
- Keep the generated markdown in sync by editing microsoft_protocol_audit.rs first and regenerating the report.
- Keep docs/microsoft/protocol-sources.toml accurate for URL, cache path, SHA256, publication date, and exact sections used.

Verification:
- cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current
- Run any focused tests added or referenced.
- Confirm every active `[[protocol]]` PDF under `docs/microsoft/cache/` is present in the report and registry, and every `[[standard]]` or `[[reference]]` PDF is present in the registry.
```

## Prompt 8: Final Completion Audit For Gap Closure

```text
You are working in C:\Development\LPE.

Goal: prove whether all gaps identified in docs/architecture/microsoft-protocol-constants-gap.md have been addressed, explicitly deferred, or correctly documented.

Required context:
- Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md, LICENSE.md.
- Read docs/architecture/microsoft-protocol-constants-gap.md.
- Read docs/architecture/microsoft-protocol-gap-backlog.md if it exists.
- Read crates/lpe-exchange/src/microsoft_protocol_audit.rs and docs/microsoft/protocol-sources.toml.

Audit requirements:
- Do not rely on intent or previous summaries.
- For every report row, identify whether its explicit gaps are:
  - implemented and tested
  - intentionally unsupported and tested/documented
  - deferred with a clear architecture rationale
  - still untriaged or weakly evidenced
- Verify cached protocol coverage:
  - every active PDF under docs/microsoft/cache/ has an id in docs/microsoft/protocol-sources.toml
  - every cached protocol id appears in docs/architecture/microsoft-protocol-constants-gap.md
  - standards-support and historical reference IDs are explicitly classified outside the generated protocol report
  - no duplicate protocol ids exist in docs/microsoft/protocol-sources.toml
- Verify report freshness with cargo test -p lpe-exchange microsoft_protocol_gap_report_is_current.

Deliverable:
- Update docs/architecture/microsoft-protocol-gap-backlog.md with final status counts and any remaining work.
- If and only if every row has a defensible status and all required tests/docs pass, state that the gap audit is complete. Otherwise list the remaining rows and why the evidence is insufficient.
```
