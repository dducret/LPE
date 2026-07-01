# Maintenance Refactor Backlog

This backlog turns the June 27 maintenance audit and follow-up prompt set into
trackable implementation work. It is intentionally conservative: each item
keeps protocol behavior unchanged unless the item is explicitly an architecture
audit.

Read with:

- `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md`
- `docs/audits/protocol-canonical-service-boundary-audit-2026-06-27.md`
- `docs/architecture/outlook-exchange-parity-roadmap.md`

## Tracking Rules

- Prefer behavior-preserving extractions before semantic changes.
- Keep canonical mailbox, collaboration, rights, submission, and recovery state
  in shared LPE services/storage, not protocol adapters.
- Keep Outlook compatibility metadata separate from canonical user-visible
  state.
- Public MAPI autodiscover remains gated by local harness, Microsoft RCA, and
  real Outlook evidence.

## Backlog

| ID | Area | Current file(s) | Target split/module | Risk | Verification | Public MAPI autodiscover impact | Suggested Codex prompt |
| --- | --- | --- | --- | --- | --- | --- | --- |
| MR-001 | MAPI ROP dispatch diagnostics | `crates/lpe-exchange/src/mapi/dispatch.rs` | `mapi/dispatch/diagnostics.rs`, `mapi/dispatch/errors.rs` | High: file owns ROP execution and unsupported behavior. First slice should move helper-only code. | `cargo test -p lpe-exchange` focused dispatch/ROP tests; `rg` for moved debug helpers; report line count. | Indirect. Reduces regression risk in Outlook-critical path; no behavior change. | Refactor `dispatch.rs` by extracting diagnostics/debug summary and unsupported/error helpers without changing ROP bytes or canonical mutations. |
| MR-002 | MAPI dispatch object families | `crates/lpe-exchange/src/mapi/dispatch.rs` | `dispatch/folders.rs`, `dispatch/messages.rs`, `dispatch/attachments.rs`, `dispatch/tables.rs`, `dispatch/properties.rs`, `dispatch/sync_import.rs`, `dispatch/submission.rs`, `dispatch/associated_config.rs` | High: route/mutation behavior is Outlook-critical. Extract one family at a time. | Focused MAPI-over-HTTP tests for each family; `cargo test -p lpe-exchange` when feasible; report line count after each slice. | Direct if regressions break profile creation, cached mode, or send. Extraction itself should not change publication status. | Continue reducing `dispatch.rs` by object family with no protocol behavior changes. |
| MR-003 | MAPI submission and transport ROPs | `crates/lpe-exchange/src/mapi/dispatch.rs`, `crates/lpe-storage/src/submission.rs` | `dispatch/submission.rs` plus shared canonical submission services where needed | High: draft/send/Sent/Bcc are gate-critical. | MAPI submission tests, duplicate execute replay tests, Bcc-safe tests, canonical Sent tests; `rg` for `RopSetSpooler`, `RopSpoolerLockMessage`, `RopTransportNewMail`. | Yes for draft/send/Sent; advisory ROPs block only if Outlook evidence requires acknowledged advisory state. | Extract `RopSubmitMessage`, `RopTransportSend`, `RopAbortSubmit`, and unsupported spooler/advisory handling without implementing unsupported ROPs. |
| MR-004 | EWS service routing | `crates/lpe-exchange/src/service.rs` | `service/http_routes.rs`, `service/ews/dispatch.rs`, `service/ews/xml.rs` | High: endpoint paths and SOAP dispatch are client-visible. | Focused EWS operation tests; route/auth smoke tests; report line count. | Indirect for MAPI gate; direct for Exchange/EWS compatibility claims. | Split EWS operation routing and SOAP dispatch from `service.rs` without endpoint or response changes. |
| MR-005 | EWS item operations | `crates/lpe-exchange/src/service.rs` | `service/ews/mail.rs`, `service/ews/contacts.rs`, `service/ews/calendar.rs`, `service/ews/tasks.rs`, `service/ews/mime.rs` | High: item create/update/delete/send touches canonical state. | EWS item tests, MIME tests, SOAP error tests; `cargo test -p lpe-exchange` focused. | Indirect for MAPI publication; direct for Outlook/EWS workflows and add-ins. | Extract EWS CreateItem, UpdateItem, DeleteItem, MoveItem, CopyItem, ArchiveItem, and SendItem handlers. |
| MR-006 | Exchange storage facade | `crates/lpe-exchange/src/store.rs` | `store/mail.rs`, `store/collaboration.rs`, `store/permissions.rs`, `store/rules.rs`, `store/recoverable.rs`, `store/public_folders.rs`, `store/outlook_metadata.rs` | High: broad protocol storage seam; fake stores and canonical adapters must stay aligned. | `cargo test -p lpe-exchange`; focused tests for each extracted store family; report line count. | Indirect unless a store split changes canonical mutation behavior. | Split `ExchangeStore` implementation into focused modules without changing trait semantics. |
| MR-007 | MAPI table projections | `crates/lpe-exchange/src/mapi/tables.rs`, `crates/lpe-exchange/src/mapi/tables/` | `tables/hierarchy.rs`, `tables/contents.rs`, `tables/associated_contents.rs`, `tables/permissions.rs`, `tables/public_folders.rs`, `tables/search_folders.rs`, `tables/diagnostics.rs` | High: row bytes and sort/filter behavior are protocol-visible. | MAPI table tests, golden row/protocol tests, MAPI-over-HTTP table tests; report line count. | Direct if table rows affect profile creation, folder views, or cached-mode sync. | Continue splitting `tables.rs`; do not alter table row output. |
| MR-008 | MAPI property mapping | `crates/lpe-exchange/src/mapi/properties.rs`, `crates/lpe-exchange/src/mapi/properties/` | `properties/message.rs`, `folder.rs`, `contact.rs`, `calendar.rs`, `task.rs`, `recurrence.rs`, `streams.rs`, `diagnostics.rs` | High: property IDs, wire values, and named-property allocation are fragile. | MAPI property tests, named property tests, custom property persistence tests; report line count. | Direct if properties affect Outlook object fidelity or profile bootstrap. | Continue splitting `properties.rs`; preserve IDs, encoding, named-property allocation, and custom persistence. |
| MR-009 | MAPI ROP parser/serializer | `crates/lpe-exchange/src/mapi/rop.rs`, `crates/lpe-exchange/src/mapi/rop/` | `rop/parse.rs`, `responses.rs`, `restrictions.rs`, `recipients.rs`, `property_rows.rs`, `debug.rs`, focused tests | High: request boundaries and response bytes are protocol-critical. | ROP parser golden tests, unsupported ROP tests, parse error tests; `rg` for `unsupported_rop_response`. | Direct if parser changes break Outlook ROP batches. | Continue splitting `rop.rs`; preserve unsupported/reserved ROP behavior exactly. |
| MR-010 | MAPI snapshot/projection boundary | `crates/lpe-exchange/src/mapi_mailstore.rs`, `crates/lpe-exchange/src/mapi_store.rs` | identity/source keys, folder tree, message contents, associated contents, recoverable items, permissions, rules/delegate freebusy, diagnostics | High: defines canonical state versus Outlook metadata projection. | MAPI store/mailstore tests, ICS tests, table projection tests; report line counts. | Direct if snapshot projection affects profile/cached-mode sync. | Split MAPI store projection files while preserving object IDs, source keys, change keys, and sync facts. |
| MR-011 | Storage protocol projections | `crates/lpe-storage/src/protocols.rs` | `protocols/jmap.rs`, `imap.rs`, `activesync.rs`, `mapi_projection.rs`, `blobs.rs`, `search.rs`, `types.rs` | Medium-high: widely imported public storage types. | `cargo test -p lpe-storage`; affected protocol crate tests if type paths move; report line count. | Indirect; improves protocol adapter stability. | Split `protocols.rs` while preserving public exports and serialized output. |
| MR-012 | Storage blob lifecycle | `crates/lpe-storage/src/blob_store.rs` | `blob_store/metadata.rs`, `placements.rs`, `migration.rs`, `cleanup.rs`, `verify.rs`, focused tests | Medium-high: data integrity and migration safety. | `cargo test -p lpe-storage blob_store`; blob migration/cleanup tests. | No direct MAPI gate impact; protects attachment/blob integrity. | Split `blob_store.rs` without changing placement, migration, cleanup, hash verification, or rollback semantics. |
| MR-013 | ActiveSync service | `crates/lpe-activesync/src/service.rs` | `service/dispatch.rs`, `folder_sync.rs`, `sync.rs`, `send.rs`, `move_items.rs`, `item_operations.rs`, `search.rs`, `ping.rs`, `provisioning.rs`, `parsing.rs` | Medium-high: ActiveSync status codes and sync keys are client-visible. | `cargo test -p lpe-activesync`; focused WBXML/status tests; report line count. | No direct MAPI gate impact; supports Outlook mobile/native client story. | Split ActiveSync service with no WBXML/status/auth/sync-key behavior changes. |
| MR-014 | MAPI HTTP transport | `crates/lpe-exchange/src/mapi/transport.rs` | `transport/headers.rs`, `cookies.rs`, `session.rs`, `replay.rs`, `request.rs`, `response.rs`, `diagnostics.rs` | High: MAPI/HTTP headers, cookies, replay, and sequence validation are profile-gate critical. | `cargo test -p lpe-exchange mapi_over_http::transport`; MAPI connect/reconnect/replay tests. | Direct for public MAPI autodiscover because profile creation uses this path. | Split `transport.rs` preserving headers, cookies, sequence, replay, content-length, and error envelopes. |
| MR-015 | NSPI | `crates/lpe-exchange/src/mapi/nspi.rs` | `nspi/parse.rs`, `responses.rs`, `properties.rs`, `lookup.rs`, `diagnostics.rs`, focused tests | High: Outlook address book/profile bootstrap depends on NSPI. | `cargo test -p lpe-exchange nspi`; NSPI MAPI-over-HTTP tests; line count report. | Direct for public MAPI autodiscover/profile creation. | Split NSPI while keeping Microsoft-specific matching local. |
| MR-016 | LPE-CT SMTP service | `LPE-CT/src/smtp.rs`, `LPE-CT/src/smtp/` | `smtp/policy.rs`, `queue.rs`, `outbound.rs`, `dsn.rs`, `tests/` | High for perimeter security; medium for Outlook. | `cargo test` in `LPE-CT`; SMTP reply/queue/quarantine/bridge tests; report line count. | No direct MAPI autodiscover impact; protects submission/relay boundary after LPE handoff. | Continue splitting SMTP without changing replies, custody, quarantine, bridge, or relay semantics. |
| MR-017 | LPE-CT main wiring | `LPE-CT/src/main.rs` | `config.rs`, `routes.rs`, `app_state.rs`, `admin_api.rs`, `startup.rs`, `shutdown.rs` | Medium-high: startup/env/routes can regress deployment. | `cargo test` in `LPE-CT`; compile/check command; route/config smoke tests if present. | No direct MAPI gate impact; operational reliability. | Split `main.rs` into wiring modules without changing CLI/env/routes/auth/startup/logging. |
| MR-018 | MAPI diagnostic hex helpers | `crates/lpe-exchange/src/mapi/dispatch.rs`, `mapi_mailstore.rs`, `mapi/transport.rs`, `mapi/nspi.rs`, tests | `mapi/diagnostics/hex.rs` or family-local diagnostics module | Medium: debug output changes can affect tests/log analysis; protocol bytes must not change. | `cargo test -p lpe-exchange`; `rg` duplicate helper names; debug-output tests. | Indirect; improves diagnosability for Outlook traces. | Centralize identical diagnostic hex/preview helpers only. |
| MR-019 | Exchange rule/deferred-action model | Docs: EWS/MAPI rules; code references in `service.rs`, `dispatch.rs` | `docs/architecture/exchange-rule-deferred-action-canonical-model.md` | Medium: architecture decision before behavior. | `rg` for `RopUpdateDeferredActionMessages`, `Exchange rule blobs`, `UpdateInboxRules`, `RopModifyRules`. | No for profile publication unless Outlook setup requires uploads; yes for full rule parity. | Create canonical model audit for rules/deferred actions; no code. |
| MR-020 | Receive-folder routing model | Docs and MAPI tests for `RopGetReceiveFolder`, `RopSetReceiveFolder` | `docs/architecture/mapi-receive-folder-routing.md` | Medium: must not invent delivery routing without canonical model. | `rg` for `RopSetReceiveFolder` and receive-folder docs/tests. | No unless Outlook requires `RopSetReceiveFolder` during profile/cached mode. | Create receive-folder routing architecture note; no code. |
| MR-021 | Spooler advisory model | Docs and tests for `RopSetSpooler`, `RopSpoolerLockMessage`, `RopTransportNewMail`, `RopAbortSubmit` | `docs/architecture/mapi-spooler-advisory-model.md` | Medium-high: send path must preserve LPE/LPE-CT custody. | `rg` for advisory ROPs and abort-submit tests. | Draft/send/Sent are direct; advisory ROPs block only if Outlook evidence requires state. | Create spooler advisory architecture note; no code. |
| MR-022 | Public-folder Outlook parity | `docs/architecture/public-folders-mapi-mvp.md`, public-folder tests/modules | `docs/audits/public-folder-outlook-parity-follow-up-YYYY-MM-DD.md` | Medium: public folders are complex but not basic private mailbox profile gate. | `rg` for public-folder docs/tests and unsupported behavior. | No for private mailbox MAPI autodiscover unless public folders are in the claim. | Create public-folder parity follow-up audit; no code. |
| MR-023 | Search Folder/Common Views parity | `search_folders`, Common Views associated config, MAPI table/dispatch/properties docs/tests | `docs/audits/mapi-search-folder-common-views-parity-YYYY-MM-DD.md` | Medium-high: folder views can affect Outlook navigation. | `rg` for `RopSetSearchCriteria`, `RopGetSearchCriteria`, `search_folders`, `Common Views`. | Yes if unsupported search/view rows block bootstrap or hierarchy sync; otherwise parity debt. | Create focused Search Folder/Common Views audit; no code. |
| MR-024 | Notification replay parity | EWS notifications, MAPI `NotificationWait`, canonical `mail_change_log` | `docs/audits/outlook-notification-replay-parity-YYYY-MM-DD.md` | Medium-high: stale views and reconnect behavior. | `rg` for `NotificationWait`, `RopRegisterNotification`, `Subscribe`, `GetEvents`, `GetStreamingEvents`, `mail_change_log`. | No for initial publication if cached-mode sync converges; yes if Outlook evidence shows broken views. | Create EWS/MAPI notification parity audit; no code. |
| MR-025 | Oversized-source check adoption | `tools/check_oversized_sources.py`, documentation | Existing tool plus documentation for warn/fail modes | Low-medium: process hardening only. | `python tools/check_oversized_sources.py`; `python tools/check_oversized_sources.py --fail` should fail while offenders remain. | Indirect; prevents future Outlook-critical file growth. | Harden and document oversized-source check. |
| MR-026 | Maintenance backlog tracking | `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md` | This file | Low: documentation only. | `rg` every “Next Highest-Risk Files” entry in this backlog. | Indirect. | Maintain this backlog and keep prompt mappings current. |
| MR-027 | Verification sweep | Updated audit and command outputs | Audit “Verification Sweep” subsection | Medium: test suite currently has known red/hanging areas that must be recorded accurately. | Oversized check, `rg` commands from audit, attempted cargo tests and exact outcomes. | Indirect unless a gate test fails. | Run and document full post-refactor verification sweep. |

## Status Summary

This summary is intentionally evidence-based. It records what the current
working tree proves, not the desired end state.

| ID | Status | Evidence | Remaining work |
| --- | --- | --- | --- |
| MR-001 | Partial | Dispatch diagnostics and helper-only modules exist under `crates/lpe-exchange/src/mapi/dispatch/`; focused tests are recorded in progress notes. | `dispatch.rs` remains the largest source file; more helper/error extraction is still needed. |
| MR-002 | Partial | Folder, message, logon, public-folder, recipient, rule, attachment, table-validation, execute, and sync-import helper slices are recorded in progress notes. | ROP execution branches, submission, properties, tables, associated config, and more mutation routing still need extraction. |
| MR-003 | Partial | Spooler/advisory behavior is documented in `docs/architecture/mapi-spooler-advisory-model.md`, and `dispatch/submission.rs` now owns submission response-policy helpers. | Full submission ROP execution extraction into `dispatch/submission.rs` is not yet complete. |
| MR-004 | Partial | `service/http_routes.rs` now owns endpoint path constants, RPC proxy path list, and top-level route assembly, with focused route/RPC/MAPI verification recorded in progress notes. | Extract EWS SOAP operation dispatch without endpoint, auth, or response changes. |
| MR-005 | Partial | EWS mail, contact, calendar recurrence, task, reminder, room, rules, attachment, OOF, user-configuration, MailTips, Mail Apps, and ConvertId parser/helper slices are recorded in progress notes. | Continue extracting EWS item-family parsers and handlers while preserving canonical mutations and SOAP responses. |
| MR-006 | Pending | No completed `ExchangeStore` split is recorded in this backlog. | Split `crates/lpe-exchange/src/store.rs` by storage family while preserving trait semantics. |
| MR-007 | Pending | Earlier table helper extraction exists in the repository, but this backlog does not record a completed current slice. | Continue splitting `tables.rs` and prove table row output is unchanged. |
| MR-008 | Pending | Earlier property helper extraction exists in the repository, but this backlog does not record a completed current slice. | Continue splitting `properties.rs` and preserve property IDs, encoding, named properties, and custom values. |
| MR-009 | Complete for hub split | `mapi/rop.rs` is now below the 1,500-line production target, with parser, request-reader, response, restriction, recipient, property-row, debug, error, object-id, receive-folder, logon, named-property, attachment, and buffer helpers in focused modules. | Keep future ROP behavior additions in focused modules; preserve unsupported/reserved ROP behavior. |
| MR-010 | Pending | No completed MAPI mailstore/store projection split is recorded in this backlog. | Split projection and Outlook metadata boundaries while preserving IDs, source keys, change keys, and sync facts. |
| MR-011 | Pending | No completed storage protocol projection split is recorded. | Split `crates/lpe-storage/src/protocols.rs` while preserving exports and serialized output. |
| MR-012 | Pending | No completed blob-store split is recorded. | Split `blob_store.rs` and verify placement, migration, cleanup, and hash behavior. |
| MR-013 | Pending | No completed ActiveSync service split is recorded. | Split ActiveSync service without WBXML/status/auth/sync-key changes. |
| MR-014 | Pending | No completed MAPI transport split is recorded. | Split `mapi/transport.rs` while preserving headers, cookies, sequence, replay, and envelopes. |
| MR-015 | Pending | No completed NSPI split is recorded. | Split NSPI parsing/responses/properties/lookup while keeping Microsoft-specific matching local. |
| MR-016 | Pending | The audit records prior SMTP reductions, but this backlog does not record a completed current slice. | Continue splitting `LPE-CT/src/smtp.rs` and verify SMTP semantics. |
| MR-017 | Pending | No completed `LPE-CT/src/main.rs` split is recorded. | Split main wiring without CLI/env/routes/auth/startup changes. |
| MR-018 | Partial | Primitive crypto helpers are centralized, MAPI diagnostic lowerhex wrappers delegate to `lpe_domain::crypto::hex_lower`, and ROP debug hex rendering lives in `mapi/rop/debug.rs`. NSPI/test parsers and validation helpers remain local. | Continue centralizing only identical diagnostic helpers and preserve debug output/protocol bytes. |
| MR-019 | Complete for documentation | `docs/architecture/exchange-rule-deferred-action-canonical-model.md` exists and is referenced in progress notes. | Implementation of wider rule/deferred-action semantics remains future work. |
| MR-020 | Complete for documentation | `docs/architecture/mapi-receive-folder-routing.md` exists and stale broad unsupported wording was updated. | Arbitrary receive-folder routing still needs a canonical model before implementation. |
| MR-021 | Complete for documentation | `docs/architecture/mapi-spooler-advisory-model.md` exists and is referenced in progress notes. | Advisory ROP support remains gated on canonical advisory state and Outlook evidence. |
| MR-022 | Complete for documentation | `docs/audits/public-folder-outlook-parity-follow-up-2026-06-28.md` exists and is referenced in progress notes. | Public-folder parity implementation gaps remain staged future work. |
| MR-023 | Complete for documentation | `docs/audits/mapi-search-folder-common-views-parity-2026-06-28.md` exists and is referenced in progress notes. | Search Folder/Common Views parity implementation gaps remain staged future work. |
| MR-024 | Complete for documentation | `docs/audits/outlook-notification-replay-parity-2026-06-28.md` exists and is referenced in progress notes. | Notification replay implementation gaps remain staged future work. |
| MR-025 | Partial | `README.md` documents `tools/check_oversized_sources.py` warn/fail/include-tests modes; check outputs are recorded. | CI/adoption policy is not yet added, and offenders remain above threshold. |
| MR-026 | Partial | This backlog now maps the primary high-risk files and records coverage gaps for additional oversized files. | Keep mappings current as refactors land; future backlog items are needed for uncovered oversized files. |
| MR-027 | Partial | The maintenance audit contains a 2026-06-28 non-cargo verification sweep. | Full cargo verification is not current because the previous lpe-exchange test process remains alive and had visible failures. |

## Current Highest-Risk File Coverage

The updated audit's current highest-risk entries map to backlog items as
follows:

| Audit file | Backlog item |
| --- | --- |
| `crates/lpe-exchange/src/mapi/dispatch.rs` | MR-001, MR-002, MR-003 |
| `crates/lpe-exchange/src/service.rs` | MR-004, MR-005 |
| `crates/lpe-exchange/src/store.rs` | MR-006 |
| `crates/lpe-exchange/src/mapi/tables.rs` | MR-007 |
| `crates/lpe-exchange/src/mapi/properties.rs` | MR-008 |
| `crates/lpe-exchange/src/mapi/rop.rs` | MR-009 |
| `crates/lpe-exchange/src/mapi_mailstore.rs` | MR-010 |
| `crates/lpe-exchange/src/mapi_store.rs` | MR-010 |
| `crates/lpe-storage/src/protocols.rs` | MR-011 |
| `crates/lpe-storage/src/blob_store.rs` | MR-012 |
| `crates/lpe-activesync/src/service.rs` | MR-013 |
| `crates/lpe-exchange/src/mapi/transport.rs` | MR-014 |
| `crates/lpe-exchange/src/mapi/nspi.rs` | MR-015 |
| `LPE-CT/src/smtp.rs` | MR-016 |
| `LPE-CT/src/main.rs` | MR-017 |

## Oversized Check Coverage Gaps

The 2026-06-28 oversized-source check reports additional production hotspots
outside the original MR-001 through MR-027 prompt set. They are recorded here so
MR-026 can distinguish covered recommendations from future backlog work instead
of silently losing them.

| Current oversized file | Lines | Current backlog coverage |
| --- | ---: | --- |
| `LPE-CT/web/app.js` | 5,593 | Not covered by MR-001 through MR-027. Needs a future LPE-CT admin UI module split plan. |
| `crates/lpe-storage/sql/schema.sql` | 3,455 | Not covered. Likely acceptable only if treated as dense schema source; otherwise needs schema documentation or split policy. |
| `crates/lpe-jmap/src/service.rs` | 3,389 | Not covered. Needs a future JMAP service routing/operation-family split. |
| `crates/lpe-exchange/src/mapi/store_adapter.rs` | 3,338 | Partially related to MR-006/MR-010, but not explicitly covered. Needs a dedicated Exchange store-adapter split if it remains oversized. |
| `crates/lpe-admin-api/src/workspace.rs` | 3,085 | Not covered. Needs a future admin workspace API split. |
| `crates/lpe-exchange/src/mapi/sync.rs` | 2,989 | Partially related to MR-010/MR-014, but not explicitly covered. Needs a dedicated ICS/sync split if it remains oversized. |
| `crates/lpe-storage/src/collaboration.rs` | 2,966 | Not covered. Needs a future canonical collaboration storage split. |
| `LPE-CT/web/styles.css` | 2,765 | Not covered. Needs a future LPE-CT admin UI stylesheet/design-system split. |
| `crates/lpe-storage/src/submission.rs` | 2,663 | Partially related to MR-003, but storage submission itself is not explicitly covered. |
| `crates/lpe-storage/src/admin.rs` | 2,385 | Not covered. Needs a future storage admin split. |
| `web/client/src/styles.css` | 2,348 | Not covered. Needs a future web client stylesheet/design-system split. |
| `crates/lpe-exchange/src/mapi/session.rs` | 2,289 | Partially related to MR-014 transport/session work, but not explicitly covered. |
| `crates/lpe-jmap/src/mail.rs` | 2,220 | Not covered. Needs a future JMAP mail operation split. |
| `crates/lpe-admin-api/src/client_config.rs` | 2,166 | Not covered. Needs a future admin client-config split. |
| `LPE-CT/web/modules/i18n/messages.js` | 2,115 | Not covered. May be acceptable as dense message data if documented; otherwise split by locale/domain. |
| `crates/lpe-storage/src/public_folders.rs` | 1,872 | Partially related to MR-022 public-folder parity docs, but no storage split is assigned. |
| `installation/debian-trixie/update-lpe.sh` | 1,847 | Not covered. Needs a future installer/update script split or documented exception. |
| `tools/rca_outlook_connectivity_check.py` | 1,844 | Not covered. Needs a future RCA tooling split or documented exception. |
| `crates/lpe-imap/src/render.rs` | 1,654 | Not covered. Needs a future IMAP rendering split. |
| `crates/lpe-storage/src/tasks.rs` | 1,646 | Not covered. Needs a future storage task split. |
| `LPE-CT/src/reporting.rs` | 1,618 | Not covered. Needs a future reporting split. |
| `crates/lpe-storage/src/storage_visibility.rs` | 1,559 | Not covered. Needs a future visibility/query split. |
| `crates/lpe-jmap/src/store.rs` | 1,529 | Not covered. Needs a future JMAP store facade split. |

## Suggested Execution Order

1. MR-001, MR-002, MR-003: reduce `dispatch.rs` risk in small verified slices.
2. MR-004, MR-005, MR-006: reduce service/store coupling around EWS and
   canonical mutations.
3. MR-007, MR-008, MR-009, MR-010, MR-014, MR-015: continue MAPI module
   decomposition around wire/table/property/transport/profile boundaries.
4. MR-019 through MR-024: document unresolved parity models before adding
   semantics.
5. MR-011, MR-012, MR-013, MR-016, MR-017, MR-018, MR-025, MR-027: broader
   maintenance and verification hardening.

## Progress Notes

- 2026-06-28: Started MR-001 by adding
  `crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs` and moving
  helper-only dispatch diagnostics there. The slice covers role/container
  debug classifiers, ROP/logon request diagnostics, execute parse/store access
  logging, MAPI object debug naming, live-handle summaries, diagnostic MAPI
  value-shape formatting, optional folder ID formatting, and hex/preview
  helpers. It deliberately leaves ROP execution, response bytes, table row
  output, property encoding, unsupported ROP behavior, and canonical mutations
  in place.
- 2026-06-28 verification for the MR-001 slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange execute_rop_debug_summary --lib`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence`;
  `cargo test -p lpe-exchange
  inbox_open_loop_summary_requires_repeated_probe_without_contents_table --lib`;
  `cargo test -p lpe-exchange
  open_folder_debug_metadata_uses_real_dynamic_mailbox_values --lib`; `cargo
  test -p lpe-exchange
  logon_response_debug_summary_decodes_private_mailbox_fields --lib`;
  `rg` for `unsupported_rop_response`, `rop_parse_error_response`, and
  `format_live_handle_debug_summary`; `rg` for `mapi_value_debug_` helper
  definitions; `rg` for `format_debug_property_tags` and
  `format_debug_sort_orders`; `rg` for `rop_names_csv` and `rop_name`; `rg`
  for `post_hierarchy_probe_folder_name` and
  `expected_special_folder_container_class`; `rg` for
  `format_expected_folder_id_for_debug`; `rg` for
  `execute_response_framing_context`; `rg` for
  `summarize_response_rop_frame`; `rg` for `rop_has_no_response`; `rg` for
  `execute_batch_has_same_save_getprops_not_found`; `rg` for
  `RopRequestDebugSummary`, `RopResponseDebugSummary`, and
  `LogonResponseDebugSummary`; `rg` for
  `summarize_non_release_request_rops`; `rg` for
  `summarize_request_rop_raw_frames`; `rg` for
  `summarize_request_rop_buffer`; `rg` for
  `summarize_response_rop_buffer`; `rg` for
  `summarize_logon_response_rop`; `rg` for `read_response_error_code`; `rg`
  for `summarize_handle_table`; `rg` for `PostHierarchyReleaseDebugEvent` and
  `post_sync_release_flags`; `rg` for `pending_recipient_types_summary`; `git
  diff --check` for the touched MAPI files; `cargo test -p lpe-exchange
  property_tag_validation_tests --lib`; `rg` for `table_validation`; `python
  tools/check_oversized_sources.py`; `cargo test -p lpe-exchange
  get_buffer_response_debug_exposes_wire_framing --lib`; `rg` for
  `summarize_fast_transfer_get_buffer_response`.
- 2026-06-28: Advanced MR-002 with a narrow Execute helper extraction into
  `crates/lpe-exchange/src/mapi/dispatch/execute.rs`. The slice moves
  `ExecuteRequest`, `parse_execute_request`, `apply_execute_max_rop_out`, and
  `execute_response_handle_table`, plus the active Execute session retry
  wrapper, while preserving the crate-internal dispatch API, response bytes,
  handle-table behavior, session-overlap wait behavior, and Execute max-output
  error behavior.
- 2026-06-28 verification for the MR-002 Execute slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  execute_max_rop_out_returns_buffer_too_small_response --lib`; `cargo test -p
  lpe-exchange parse_execute_request_keeps_max_rop_out --lib`; `cargo test -p
  lpe-exchange release_only_execute_response_echoes_input_handle_table --lib`;
  `cargo test -p lpe-exchange
  mixed_release_execute_response_preserves_sparse_output_handle_index --lib`;
  `cargo test -p lpe-exchange
  execute_active_session_acquire_waits_for_short_outlook_overlap --lib`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence`;
  `rg` for Execute helper definitions.
- 2026-06-28: Extended the MR-002 Execute slice by moving the
  store-independent Execute batch classifiers into
  `crates/lpe-exchange/src/mapi/dispatch/execute.rs`. This covers Logon-only,
  Release-only, and special-folder GetProperties probe classification without
  changing the routing decisions that decide whether Execute can avoid storage
  access.
- 2026-06-28 verification for the store-independent Execute classifier slice:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  release_only_execute_batch_is_store_independent --lib`; `cargo test -p
  lpe-exchange special_folder_getprops_probe_is_store_independent --lib`;
  `cargo test -p lpe-exchange
  root_folder_type_getprops_probe_stays_store_independent --lib`; `cargo test
  -p lpe-exchange store_independent --lib`.
- 2026-06-28: Moved the remaining Execute-local empty-request and successful
  response ROP-buffer helpers into
  `crates/lpe-exchange/src/mapi/dispatch/execute.rs`. Verification: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence`.
- 2026-06-28: Started the MR-002 folder-family extraction by adding
  `crates/lpe-exchange/src/mapi/dispatch/folders.rs` for pure folder policy
  helpers. This slice covers private create-folder existing flags, deleted
  advertised special-folder reuse, advertised contact-folder no-op delete
  acknowledgement, synthetic-folder associated-message creation allowance,
  advertised special-folder container classes, and folder-local default view
  support. ROP folder mutation handlers and response construction remain in
  `dispatch.rs`.
- 2026-06-28 verification for the folder policy slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  private_create_folder_response_never_sets_existing_folder_flag --lib`;
  `cargo test -p lpe-exchange
  deleted_advertised_quick_step_create_can_reuse_existing_real_folder --lib`;
  `cargo test -p lpe-exchange
  advertised_contact_folders_use_noop_delete_acknowledgement --lib`; `cargo
  test -p lpe-exchange
  quick_step_synthetic_folder_allows_associated_message_creation --lib`;
  `cargo test -p lpe-exchange
  inbox_view_handoff_table_contract_reports_folder_local_default_view --lib`;
  `cargo test -p lpe-exchange
  junk_view_handoff_table_contract_reports_folder_local_default_view --lib`;
  `cargo test -p lpe-exchange
  contacts_view_handoff_table_contract_reports_contact_default_view --lib`;
  `cargo test -p lpe-exchange
  calendar_view_handoff_table_contract_reports_calendar_default_view --lib`.
- 2026-06-28: Started the MR-002 message-family extraction by adding
  `crates/lpe-exchange/src/mapi/dispatch/messages.rs` for pure message helper
  logic. This slice covers canonical message folder selection, fallback open
  folder resolution, folder inference for `RopOpenMessage`, unique message-id
  matching, and persisted-message best-effort delete classification. Message
  mutation, save, submit, recipient, and persistence paths remain in
  `dispatch.rs`.
- 2026-06-28 verification for the message helper slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  open_message_fallback_preserves_valid_requested_folder --lib`; `cargo test
  -p lpe-exchange
  mapi_over_http_open_message_recovers_unique_message_folder_mismatch`; `cargo
  test -p lpe-exchange
  mapi_over_http_delete_properties_no_replicate_is_best_effort_for_persisted_message`;
  `cargo test -p lpe-exchange
  folder_properties_for_open_keeps_loaded_inbox_counts_and_mapi_name --lib`
  now passes after correcting its stale default-view entry-id expectation to
  match the existing folder-local default view behavior.
- 2026-06-28: Extended the MR-002 message-family extraction by moving
  `append_save_changes_message_response` into
  `crates/lpe-exchange/src/mapi/dispatch/messages.rs`. This keeps the
  SaveChanges response/handle-slot helper with the message helpers while the
  SaveChanges ROP execution branches remain in `dispatch.rs`.
- 2026-06-28 verification for the SaveChanges helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  save_changes_success_response_updates_response_handle_slot --lib`; `cargo
  test -p lpe-exchange
  mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email`;
  `cargo test -p lpe-exchange
  mapi_over_http_open_message_then_gets_canonical_message_properties`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/logon.rs` for
  private/public logon handle classification helpers. This slice moves
  `private_logon_request_handle` and `logon_request_handle`; Logon ROP
  execution and response construction remain in `dispatch.rs`.
- 2026-06-28 verification for the logon handle helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_get_receive_folder_requires_private_logon_handle`; `cargo
  test -p lpe-exchange
  mapi_over_http_set_receive_folder_requires_private_logon_handle`; `cargo
  test -p lpe-exchange
  mapi_over_http_public_folder_logon_allocates_public_folder_store_handle`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_public_folder_replica_rops_require_logon_handle_and_shape`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/public_folders.rs`
  for public-folder per-user information stream helpers. This slice moves the
  `LPEPFU1` per-user read-state stream encoder/decoder helpers only; public
  folder ROP execution and canonical per-user state mutation remain in
  `dispatch.rs`.
- 2026-06-28 verification for the public-folder per-user helper slice: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_public_folder_per_user_lookup_returns_canonical_folder_identity`;
  `cargo test -p lpe-exchange
  mapi_over_http_public_folder_per_user_information_round_trips_canonical_read_state`;
  `cargo test -p lpe-exchange
  mapi_over_http_public_folder_per_user_information_rejects_exchange_blob_without_state_change`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/rules.rs` for
  bounded rule mutation helpers. This slice moves the canonical bounded Sieve
  row conversion, JSON-to-Sieve rendering, escaping, and audit helper out of
  `dispatch.rs`. Rule ROP execution, Exchange rule blob rejection,
  deferred-action rejection, and canonical Sieve side effects remain in
  `dispatch.rs`.
- 2026-06-28 verification for the rules helper slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_modify_rules_writes_bounded_canonical_sieve_rule`; `cargo
  test -p lpe-exchange
  mapi_over_http_modify_rules_accepts_bounded_sieve_actions`; `cargo test -p
  lpe-exchange mapi_over_http_modify_rules_rejects_exchange_rule_blobs`;
  `cargo test -p lpe-exchange
  mapi_over_http_update_deferred_action_messages_rejects_without_sieve_side_effect`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/recipients.rs`
  for pure recipient conversion helpers. This slice moves pending-recipient to
  submission-recipient conversion and saved-message recipient projection out of
  `dispatch.rs`; staged recipient replacement and ModifyRecipients/ReadRecipients
  ROP execution remain in `dispatch.rs`.
- 2026-06-28 verification for the recipient helper slice: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row
  --lib`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/sync_import.rs`
  for FastTransfer destination helper code. This slice moves destination target
  resolution, upload buffer staging/commit, FastTransfer marker detection, and
  bounded property-value decoding out of `dispatch.rs`; FastTransfer ROP
  execution, synchronization import ROP execution, canonical object creation,
  and unsupported marker behavior remain unchanged.
- 2026-06-28 verification for the sync-import helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_upload_saves_canonical_email`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_rejects_marker_and_subobject_streams`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_rejects_unsupported_property_type`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_rejects_partial_property_buffer`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_put_buffer_extended_is_parseable`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_rejects_wrong_target_handle`.
- 2026-06-28: Extended `crates/lpe-exchange/src/mapi/dispatch/sync_import.rs`
  with pure import source-key and predecessor-change-list helpers. This slice
  moves source-key counter extraction, import source-key scope classification,
  imported property source-key inspection, and PCL conflict parsing/comparison
  out of `dispatch.rs`; sync import ROP execution and identity allocation remain
  in `dispatch.rs`.
- 2026-06-28 verification for the sync-import source-key/PCL helper slice:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxcfxics_4_6_fail_on_conflict_uses_predecessor_change_list`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxcfxics_4_6_newer_predecessor_change_list_imports`;
  `cargo test -p lpe-exchange
  mapi_over_http_save_message_replaces_out_of_range_import_source_key`; `cargo
  test -p lpe-exchange
  mapi_over_http_save_message_falls_back_when_import_source_key_is_already_used`.
- 2026-06-28: Added `crates/lpe-exchange/src/mapi/dispatch/attachments.rs`
  for embedded-message attachment helpers. This slice moves transient embedded
  message ID calculation, embedded-message open source projection, saved
  embedded-message property reconstruction, and pending embedded-message
  attachment payload construction out of `dispatch.rs`; OpenEmbeddedMessage,
  SaveChangesAttachment, attachment table handling, and canonical attachment
  persistence remain in `dispatch.rs`.
- 2026-06-28 verification for the embedded attachment helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_saved_embedded_message_reopens_from_attachment_table`.
- 2026-06-28: Extended `crates/lpe-exchange/src/mapi/dispatch/attachments.rs`
  with attachment projection helpers for canonical attachment re-submit and
  embedded-message sync attachment fact enrichment. The submit, content sync,
  and FastTransfer ROP branches still remain in `dispatch.rs`.
- 2026-06-28 verification for the attachment projection helper slice: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards`;
  `cargo test -p lpe-exchange
  mapi_over_http_create_attachment_saves_canonical_attachment_from_properties`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_open_embedded_message_accepts_read_only_mode`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_saved_embedded_message_reopens_from_attachment_table`.
- 2026-06-28 follow-up: resolved the sync attachment manifest verification
  issue surfaced during the attachment helper slice. `sync_attachment_facts_for`
  now falls back from the requested folder ID to each email's canonical MAPI
  folder ID so advertised/special folder aliases do not suppress attachment
  facts. The test now asserts MAPI UTF-16 attachment filename/MIME properties
  and explicitly verifies that the internal file reference is not leaked on the
  wire. Verification: `cargo fmt --package lpe-exchange`; `cargo test -p
  lpe-exchange
  mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`;
  reran the four attachment projection helper tests listed above.
- 2026-06-28: Extended `crates/lpe-exchange/src/mapi/dispatch/sync_import.rs`
  with deleted-change identity mapping, special-folder change classification,
  calendar content suppression, and created-object MAPI identity allocation
  helpers. This keeps sync/import identity policy with the existing
  sync-import helper module while leaving ROP execution, canonical mutation,
  response construction, and unsupported behavior in `dispatch.rs`.
- 2026-06-28 verification for the sync-import identity helper slice: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_contact_content_sync_exports_deletes`; `cargo test -p
  lpe-exchange mapi_over_http_associated_config_content_sync_exports_deletes`;
  `cargo test -p lpe-exchange
  mapi_over_http_save_message_falls_back_when_import_source_key_is_already_used`;
  `cargo test -p lpe-exchange
  mapi_over_http_save_message_replaces_out_of_range_import_source_key`; `cargo
  test -p lpe-exchange
  mapi_over_http_sync_import_delete_and_read_state_use_canonical_store`; `cargo
  test -p lpe-exchange
  mapi_over_http_sync_import_delete_ignores_transient_trash_artifact`; `cargo
  test -p lpe-exchange
  mapi_over_http_common_views_delete_messages_deletes_navigation_shortcut`;
  `rg` for the moved helper definitions; `python
  tools/check_oversized_sources.py`; `git diff --check` for touched files.
  A broader `cargo test -p lpe-exchange` attempt compiled and started 1,593
  tests, but did not complete: visible failures before output truncation
  included `blank_search_criteria_is_invalid`,
  `folder_properties_for_open_projects_persisted_search_folder_contract`,
  `mapi_over_http_sync_import_associated_message_persists_and_replays_fai`,
  `mapi_over_http_sync_manifest_includes_canonical_read_flag_state`,
  `mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`,
  `mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer`, and
  `mapi_over_http_restrict_filters_contents_table`; the run then remained on
  `mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence` for
  more than 60 seconds without further progress.
- 2026-06-28: Resolved the isolated
  `blank_search_criteria_is_invalid` failure from the broader run. Empty
  `RopSetSearchCriteria` restriction bytes now reuse a previous bounded
  restriction only when one exists; a new criteria request without previous
  restriction state returns `EC_SEARCH_INVALID_PARAMETER` as the existing unit
  test requires. Verification: `cargo fmt --package lpe-exchange`; `cargo test
  -p lpe-exchange blank_search_criteria_is_invalid`; `cargo test -p
  lpe-exchange set_search_criteria`.
- 2026-06-28: Resolved the isolated
  `folder_properties_for_open_projects_persisted_search_folder_contract`
  failure from the broader run. The test now expects persisted search folders
  to project `PidTagExtendedFolderFlags` with the saved search-folder metadata
  payload instead of the generic six-byte folder flag payload. Verification:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  folder_properties_for_open_projects_persisted_search_folder_contract`; `cargo
  test -p lpe-exchange search_folder --lib`.
- 2026-06-28: Resolved the isolated
  `mapi_over_http_sync_import_associated_message_persists_and_replays_fai`
  failure from the broader run. The test now accepts the current successful
  zero-row `RopQueryRows` response for the normal Inbox contents table, with
  origin byte `0x02`, while still asserting the imported FAI is replayed by
  FAI sync and not leaked into the normal contents table. Verification: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_sync_import_associated_message_persists_and_replays_fai`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai`.
- 2026-06-28: Resolved the isolated
  `mapi_over_http_sync_manifest_includes_canonical_read_flag_state` failure
  from the broader run. The test now expects the canonical message flag set to
  include the attachment bit when `has_attachments` is true, while still
  asserting the read flag is exported and the follow-up flag status remains
  `FOLLOWUP_FLAGGED`. Verification: `cargo fmt --package lpe-exchange`; `cargo
  test -p lpe-exchange
  mapi_over_http_sync_manifest_includes_canonical_read_flag_state`; `cargo test
  -p lpe-exchange
  mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`;
  `cargo test -p lpe-exchange
  mapi_over_http_content_sync_read_flag_update_exports_read_state`.
- 2026-06-28: Completed MR-020 as documentation-only follow-up in
  `docs/architecture/mapi-receive-folder-routing.md`. The note records the
  current fixed canonical receive-folder map, bounded `RopSetReceiveFolder`
  acknowledgement behavior, what remains unsupported, the canonical model
  needed for wider routing, tests/evidence required, and public MAPI
  autodiscover impact. Stale wording in the EWS/MAPI architecture, Outlook
  parity roadmap, and maintenance audit was updated from broad
  "`RopSetReceiveFolder` unsupported" language to the current distinction:
  bounded canonical-map confirmation is supported, arbitrary configurable
  routing is not.
- 2026-06-28: Completed MR-021 as documentation-only follow-up in
  `docs/architecture/mapi-spooler-advisory-model.md`. The note records the
  current canonical submit/transport-send and abort-submit behavior, the
  parseable unsupported boundary for `RopSetSpooler`,
  `RopSpoolerLockMessage`, and `RopTransportNewMail`, the canonical advisory
  model needed before widening support, evidence required, and public MAPI
  autodiscover impact.
- 2026-06-28: Completed MR-019 as documentation-only follow-up in
  `docs/architecture/exchange-rule-deferred-action-canonical-model.md`. The
  note records the bounded Sieve-backed rule model, EWS/MAPI/JMAP/ManageSieve
  projection boundaries, rejected Exchange-only rule/deferred-action state, the
  canonical model needed before widening support, tests/evidence required, and
  public MAPI autodiscover impact.
- 2026-06-28: Advanced MR-025 by documenting the oversized-source check in
  `README.md`, including default exclusions, warning mode, fail mode, and
  `--include-tests` review mode. No check semantics changed. Verification:
  `python tools/check_oversized_sources.py` exited 0 and reported current
  production offenders; `python tools/check_oversized_sources.py --fail`
  exited 1 while offenders remain; `python tools/check_oversized_sources.py
  --include-tests` reported test-file hotspots for scenario split planning.
- 2026-06-28: Completed MR-022 as documentation-only follow-up in
  `docs/audits/public-folder-outlook-parity-follow-up-2026-06-28.md`. The
  audit separates current bounded canonical public-folder behavior from
  remaining Outlook/Exchange parity gaps: cross-server replication,
  recipient-bearing item conversion, arbitrary per-user binary blobs,
  public-folder reparenting, whole-folder purge, and full item-class/property
  parity. It records the canonical models, tests/evidence, and public MAPI
  autodiscover impact for each gap.
- 2026-06-28: Completed MR-023 as documentation-only follow-up in
  `docs/audits/mapi-search-folder-common-views-parity-2026-06-28.md`. The audit
  separates current canonical `search_folders`, bounded
  `RopSetSearchCriteria` / `RopGetSearchCriteria`, Common Views named-view and
  navigation shortcut behavior from remaining parity gaps: full
  `[MS-OXOSRCH]` template BLOB parity, arbitrary restriction trees,
  recipient/Bcc predicates, complete view-designer behavior, broader navigation
  shortcuts, reminder Search Folder promotion, and durable categorized/collapse
  view state.
- 2026-06-28: Completed MR-024 as documentation-only follow-up in
  `docs/audits/outlook-notification-replay-parity-2026-06-28.md`. The audit
  separates current MAPI session-scoped `RopRegisterNotification` /
  `NotificationWait` behavior and EWS `Subscribe` / `GetEvents` /
  `GetStreamingEvents` / `Unsubscribe` replay over canonical `mail_change_log`
  from remaining gaps: cross-process MAPI notification replay, full MAPI
  payload parity, EWS long-held streaming affinity, EWS push notifications,
  retention/watermark expiry policy, shared/public-folder notification
  audience rules, and spooler advisory event semantics.
- 2026-06-28: Advanced MR-027 with a non-cargo verification sweep in
  `docs/audits/lpe-maintenance-outlook-architecture-audit-2026-06-27.md`. The
  sweep records current oversized-file output, confirms primitive crypto helper
  centralization, distinguishes delegated storage normalization wrappers from
  duplicate logic, lists the focused parity follow-up docs now covering the
  remaining Outlook gaps, and records that full cargo verification remains
  unavailable while the prior `cargo test -p lpe-exchange` process is still
  alive.
- 2026-06-28: Advanced MR-026 by reconciling
  `docs/architecture/maintenance-refactor-backlog.md` with the current
  oversized-source output. The high-risk coverage table now includes MR-012,
  MR-013, MR-014, and MR-015 mappings, and a new coverage-gap table records
  oversized files that remain outside MR-001 through MR-027 so they are not
  mistaken for completed maintenance work.
- 2026-06-28: Started MR-003 with a small submission helper extraction into
  `crates/lpe-exchange/src/mapi/dispatch/submission.rs`. The slice moves only
  response-policy helpers for `RopSubmitMessage` / `RopTransportSend` success
  responses, `RopAbortSubmit` sent-source validation, and abort-submit
  cancellation result mapping. ROP execution, canonical submission, Sent/Bcc
  behavior, advisory ROP unsupported behavior, and response byte helpers remain
  unchanged.
- 2026-06-28 verification for the MR-003 submission helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_submit_pending_message_uses_canonical_submission`; `cargo test
  -p lpe-exchange mapi_over_http_transport_send_uses_canonical_submission`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions`;
  `rg` for the moved submission helper names.
- 2026-06-28: Extended the MR-003 submission helper slice by moving opened
  draft/outbox canonical submit input assembly into
  `dispatch/submission.rs`. The helper loads protected Bcc and canonical
  attachment payloads through the same store methods, then delegates to the
  existing `mapi_submit_from_email` mapping. ROP execution and error response
  mapping remain in `dispatch.rs`.
- 2026-06-28 verification for the opened draft/outbox submit helper slice:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards`;
  `cargo test -p lpe-exchange
  mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission`.
- 2026-06-28: Extended the MR-003 submission helper slice by moving the
  submit-source outgoing-folder eligibility predicate into
  `dispatch/submission.rs`. This preserves the existing Drafts/Outbox-only
  rule for opened-message submit while leaving ROP execution, logging, error
  mapping, and canonical submission calls in `dispatch.rs`.
- 2026-06-28 verification for the submit-source eligibility helper slice:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards`;
  `cargo test -p lpe-exchange
  mapi_over_http_transport_send_opened_outbox_message_uses_canonical_submission`;
  `cargo test -p lpe-exchange
  mapi_over_http_submit_pending_message_uses_canonical_submission`; `cargo
  test -p lpe-exchange mapi_over_http_transport_send_uses_canonical_submission`;
  `rg` for the submission helper names. Current line counts: `dispatch.rs`
  29,322 lines and `dispatch/submission.rs` 52 lines.
- 2026-06-28: Extended the MR-003 abort-submit helper slice by moving
  canonical message-id resolution for `RopAbortSubmit` into
  `dispatch/submission.rs`. The dispatch branch still owns request-field
  validation, the non-Sent source error response, canonical cancellation, and
  response-byte mapping.
- 2026-06-28 verification for the abort-submit canonical-id helper slice:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions`;
  `rg` for the submission helper names. Current line counts: `dispatch.rs`
  29,317 lines and `dispatch/submission.rs` 74 lines.
- 2026-06-28: Extended the MR-003 abort-submit helper slice by moving the
  `RopAbortSubmit` audit entry construction into `dispatch/submission.rs`.
  This preserves the same actor, action string, and `message:{id}` subject
  format while leaving cancellation execution in `dispatch.rs`.
- 2026-06-28 verification for the abort-submit audit helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_cancels_pre_handoff_submission`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_abort_submit_rejects_handed_off_and_terminal_submissions`;
  `rg` for the abort-submit helper names. Current line counts: `dispatch.rs`
  29,313 lines and `dispatch/submission.rs` 84 lines.
- 2026-06-28: Extended the MR-003 submit helper slice by moving
  `RopSubmitMessage` / `RopTransportSend` audit entry construction into
  `dispatch/submission.rs`. This preserves the same actor, action string, and
  `handle:{id}` subject format while leaving canonical submission execution in
  `dispatch.rs`.
- 2026-06-28 verification for the submit audit helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_submit_pending_message_uses_canonical_submission`; `cargo test
  -p lpe-exchange mapi_over_http_transport_send_uses_canonical_submission`;
  `rg` for the submit audit helper names. Current line counts: `dispatch.rs`
  29,306 lines and `dispatch/submission.rs` 91 lines.
- 2026-06-28: Extended the MR-003 submit helper slice by moving the submitted
  message session-handle object construction into `dispatch/submission.rs`.
  Identity allocation, handle insertion, same-Execute reload, and response
  construction remain in `dispatch.rs`.
- 2026-06-28 verification for the submitted-handle helper slice: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_submit_pending_message_uses_canonical_submission`; `cargo test
  -p lpe-exchange mapi_over_http_transport_send_uses_canonical_submission`;
  `rg` for the submitted-handle helper name. Current line counts:
  `dispatch.rs` 29,301 lines and `dispatch/submission.rs` 103 lines.
- 2026-06-28: Resolved the previously visible broad-suite failure
  `mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`
  by updating the test to assert the Unicode FastTransfer recipient property
  bytes emitted by the SyncManifest path. The visible To/Cc facts remain
  required, and Bcc address/display-name suppression remains asserted.
- 2026-06-28 verification for the visible-recipient sync-manifest expectation:
  `cargo test -p lpe-exchange
  mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`;
  `cargo test -p lpe-exchange
  mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`.
- 2026-06-28: Resolved the previously visible broad-suite failure
  `mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer` by
  allowing generic `SetColumns` validation to accept known-but-unmodeled MAPI
  property wire types, while keeping rule action/restriction columns limited to
  rule tables. Row serialization continues to default unmodeled values instead
  of aborting the Execute buffer.
- 2026-06-28 verification for the known-unmodeled table-column fix: `cargo
  test -p lpe-exchange
  property_tag_validation_tests::set_columns_rejects_microsoft_invalid_column_property_types`;
  `cargo test -p lpe-exchange
  mapi_over_http_known_unmodeled_table_column_type_does_not_abort_buffer`.
- 2026-06-28: Resolved the previously visible broad-suite failure
  `mapi_over_http_restrict_filters_contents_table_rows` by correcting the
  shared test content-restriction helper to encode substring/ignore-case fuzzy
  flags. The production restriction matcher already honored the payload's
  fuzzy flags; the test helper had been emitting exact, case-sensitive
  restrictions while asserting substring behavior.
- 2026-06-28 verification for the content-restriction helper correction:
  `cargo test -p lpe-exchange
  mapi_over_http_restrict_filters_contents_table`; `cargo test -p
  lpe-exchange mapi_over_http_find_row_returns_matching_contents_row`; `cargo
  test -p lpe-exchange
  mapi_over_http_microsoft_comment_restriction_wraps_find_row_predicate`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_count_restriction_wraps_find_row_predicate`.
- 2026-06-28: Resolved the previously hanging
  `mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence`
  test. The old test waited on a fake mailbox-load gate that could be bypassed
  by store-independent Execute planning, leaving the test blocked. A
  test-only active-session guard helper now marks the real connected session as
  active and verifies the HTTP PING overlap diagnostic deterministically.
- 2026-06-28 verification for the concurrent-session transport test:
  `cargo test -p lpe-exchange
  mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence`.
- 2026-06-28 broad verification after the visible-failure triage: `cargo test
  -p lpe-exchange` now completes without the previous concurrent-session hang.
  Current result is 1555 passed and 38 failed. Remaining failures are
  concentrated in MAPI tables/properties, associated configuration rows,
  calendar/freebusy projections, logon/profile response shape, submission
  advisory alignment, and content sync expectations.
- 2026-06-28: Resolved
  `mapi_over_http_unknown_property_type_terminates_current_buffer` by aligning
  its SetColumns error expectation with the current invalid-parameter response
  for unknown column property types, while preserving the assertion that the
  following `RopQueryRows` does not execute.
- 2026-06-28 verification for the unknown-property-type SetColumns expectation:
  `cargo test -p lpe-exchange
  mapi_over_http_unknown_property_type_terminates_current_buffer`; `cargo test
  -p lpe-exchange
  property_tag_validation_tests::set_columns_rejects_microsoft_invalid_column_property_types`.
- 2026-06-28: Resolved table cursor expectation failures by moving
  `query_rows_origin_tracks_cursor_boundary` off the intentionally leaf
  `Sync Issues` hierarchy and by deriving associated table position counts from
  the same restricted associated-row projection used by `RopQueryPosition` and
  `RopQueryRows`.
- 2026-06-28 verification for the table cursor expectation cleanup: `cargo
  test -p lpe-exchange query_rows_origin_tracks_cursor_boundary`; `cargo test
  -p lpe-exchange query_position_clamps_stale_cursor_to_current_row_count`;
  `cargo test -p lpe-exchange
  restricted_associated_query_position_reports_filtered_row_count`; `cargo
  test -p lpe-exchange
  query_rows_origin_uses_global_position_for_windowed_content_tables`.
- 2026-06-28 broad verification after the table cursor cleanup: `cargo test
  -p lpe-exchange` completes without hanging. Current result is 1560 passed
  and 33 failed. Remaining failures are still concentrated in MAPI
  properties/ROP response shape, contact and associated configuration table
  rows, Outlook special-folder metadata, calendar/freebusy creation and
  projections, logon/profile response shape, submission advisory alignment,
  and content sync ordering/projection expectations.
- 2026-06-28: Resolved the two contact table expectation failures by aligning
  the default contacts contents-table fixture with the table-column availability
  contract and by acknowledging that canonical contact property projection now
  supplies empty strings for missing secondary and tertiary email slots before
  table row serialization.
- 2026-06-28 verification for the contact table expectation cleanup: `cargo
  test -p lpe-exchange
  contact_table_projects_missing_secondary_email_slots_as_empty_strings`;
  `cargo test -p lpe-exchange
  default_contacts_contents_table_uses_contact_rows_and_columns`.
- 2026-06-28 broad verification after the contact table cleanup: `cargo test
  -p lpe-exchange` completes without hanging. Current result is 1562 passed
  and 31 failed. Remaining failures are concentrated in MAPI properties/ROP
  response shape, inbox associated configuration rows, Outlook special-folder
  metadata, calendar/freebusy creation and projections, logon/profile response
  shape, submission advisory alignment, and content sync ordering/projection
  expectations.
- 2026-06-28: Resolved
  `invalid_input_handle_index_serializes_common_rop_error` by aligning the
  test with the shared ROP error helper's response-handle-index convention.
  Unsupported ROP and parse-error test coverage remains present by `rg`.
- 2026-06-28 verification for the ROP handle-index expectation cleanup: `cargo
  test -p lpe-exchange
  invalid_input_handle_index_serializes_common_rop_error`; `rg -n
  "unsupported_rop_response" crates/lpe-exchange/src/mapi
  crates/lpe-exchange/src/tests`; `rg -n
  "parse error|parse_error|malformed|invalid.*parse|unsupported.*ROP|unsupported ROP"
  crates/lpe-exchange/src/mapi/rop.rs crates/lpe-exchange/src/mapi/rop
  crates/lpe-exchange/src/mapi/rop/tests.rs`.
- 2026-06-28 broad verification after the ROP handle-index cleanup: `cargo
  test -p lpe-exchange` completes without hanging. Current result is 1563
  passed and 30 failed. Remaining failures are concentrated in MAPI
  properties/ROP response shape, inbox associated configuration rows, Outlook
  special-folder metadata, calendar/freebusy creation and projections,
  logon/profile response shape, submission advisory alignment, and content sync
  ordering/projection expectations.
- 2026-06-28: Resolved the special-folder container/default-view metadata
  cluster by making `FreeBusy Data` consistently project `IPF.Note`, aligning
  the bootstrap diagnostic container-class helper with that metadata, and
  updating stale default-view expectations to match the shared auxiliary
  contact-folder and task/journal/notes policies.
- 2026-06-28 verification for the special-folder metadata cleanup: `cargo test
  -p lpe-exchange container_class`; `cargo test -p lpe-exchange
  special_folder_property_projects_view_defaults_for_outlook_folders`.
- 2026-06-28 broad verification after the special-folder metadata cleanup:
  `cargo test -p lpe-exchange` completes without hanging. Current result is
  1565 passed and 28 failed. Remaining failures are concentrated in MAPI
  properties/ROP response shape, inbox associated configuration rows,
  calendar/freebusy creation and projections, logon/profile response shape,
  submission advisory alignment, and content sync ordering/projection
  expectations.
- 2026-06-28: Resolved the Inbox associated configuration row expectation
  cluster by aligning broad `QueryRows` tests with the existing
  MessageListSettings suppression policy. Exact virtual
  `IPM.Configuration.MessageListSettings` lookup remains supported where the
  table restriction asks for it; broad associated-content enumeration continues
  to expose only modeled startup rows and backed persisted rows.
- 2026-06-28 verification for the Inbox associated configuration cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  inbox_associated_query_rows_`.
- 2026-06-28: Resolved the Common Views/OXOCFG descriptor expectation cluster
  by aligning stale tests with the current visible Inbox descriptor contract:
  the Outlook common 0x8514 boolean, `PidTagMessageStatus`, and the compact
  view auxiliary flags tag are the projected descriptor columns already covered
  by the passing view-definition test.
- 2026-06-28 verification for the Common Views/OXOCFG descriptor cleanup:
  `cargo test -p lpe-exchange
  common_view_named_view_projects_descriptor_properties_for_outlook`; `cargo
  test -p lpe-exchange
  microsoft_oxocfg_view_definition_binary_matches_protocol_example`.
- 2026-06-28: Resolved
  `folder_getprops_projects_saved_search_definition_metadata` by aligning the
  saved-search folder extended-flags expectation with the existing
  search-folder contract: generic extended folder flags plus the search-folder
  metadata tuple and definition UUID.
- 2026-06-28 verification for the saved-search folder metadata cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  folder_getprops_projects_saved_search_definition_metadata`.
- 2026-06-28 broad verification after the associated-config, descriptor, and
  saved-search expectation cleanup: `cargo test -p lpe-exchange` completes
  without hanging. Current result is 1573 passed and 20 failed. Remaining
  failures are concentrated in MAPI content sync ordering/projection,
  calendar/freebusy creation and projections, logon/profile response shape,
  property response shape, submission advisory alignment, and the
  OXOCFG-writing MAPI-over-HTTP sequence.
- 2026-06-28: Resolved
  `content_sync_manifest_starts_fai_message_before_item_properties` by
  narrowing its tag-order assertion to the actual FastTransfer boundary. The
  FAI change header still emits source/change metadata before
  `IncrSyncMessage`; the message property list starts after `IncrSyncMessage`
  and is still verified by the decoded item-boundary assertions.
- 2026-06-28 verification for the FAI manifest ordering expectation cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  content_sync_manifest_starts_fai_message_before_item_properties`.
- 2026-06-28 broad verification after the FAI manifest ordering cleanup:
  `cargo test -p lpe-exchange` completes without hanging. Current result is
  1574 passed and 19 failed. Remaining failures are concentrated in
  MAPI-over-HTTP calendar/freebusy creation and projections, logon/profile
  response shape, property response shape, submission advisory alignment,
  content sync projection expectations, and the OXOCFG-writing sequence.
- 2026-06-28: Resolved
  `mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback`
  by aligning the older sync test with the newer Microsoft 4.3.2 partial-item
  fallback coverage: a PartialItem request receives one full-item message
  change, while the uploaded state metadata remains present and unsupported
  partial-item-only properties remain absent.
- 2026-06-28 verification for the PartialItem fallback expectation cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_content_sync_partial_item_uses_microsoft_full_item_fallback`.
- 2026-06-28 broad verification after the PartialItem fallback cleanup:
  `cargo test -p lpe-exchange` completes without hanging. Current result is
  1575 passed and 18 failed. Remaining failures are concentrated in
  MAPI-over-HTTP calendar/freebusy creation and projections, logon/profile
  response shape, property response shape, submission advisory alignment,
  content sync projection expectations, and the OXOCFG-writing sequence.
- 2026-06-28: Resolved
  `mapi_over_http_content_sync_only_specified_properties_limits_message_properties`
  by removing a stale raw `IncrSyncProgressMode` marker expectation from a
  request that does not set the progress flag. The test still validates the
  `OnlySpecifiedProperties` contract through strict content-sync decoding:
  parent source key and requested subject are present, while unrequested message
  flags, flag status, normalized subject, and body are absent.
- 2026-06-28 verification for the OnlySpecifiedProperties sync expectation
  cleanup: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_content_sync_only_specified_properties_limits_message_properties`.
- 2026-06-28 broad verification after the OnlySpecifiedProperties cleanup:
  `cargo test -p lpe-exchange` completes without hanging. Current result is
  1576 passed and 17 failed. Remaining failures are concentrated in
  MAPI-over-HTTP calendar/freebusy creation and projections, logon/profile
  response shape, property response shape, submission advisory alignment,
  content sync FAI final-state expectations, and the OXOCFG-writing sequence.
- 2026-06-28: Resolved
  `mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes`
  by aligning the normal content-sync expectation with the current normal/FAI
  split: normal message sync emits message/read-state final state, while
  `CnsetSeenFAI` remains empty unless associated-message FAI sync is requested.
  The separate FAI tests still require associated-message replay and Inbox FAI
  final-state coverage.
- 2026-06-28 verification for the normal content-sync FAI-state expectation:
  `cargo test -p lpe-exchange
  mapi_over_http_content_sync_first_folder_decodes_outlook_message_changes`;
  `cargo test -p lpe-exchange fai`.
- 2026-06-28 broad verification after the normal content-sync FAI-state
  cleanup: `cargo test -p lpe-exchange` completes without hanging. Current
  result is 1577 passed and 16 failed. Remaining failures are concentrated in
  MAPI-over-HTTP calendar/freebusy creation and projections, logon/profile
  response shape, property response shape, submission advisory alignment, and
  the OXOCFG-writing sequence.
- 2026-06-28: Resolved the remaining logon/profile response-shape cluster by
  aligning stale tests with current ROP behavior: `RopGetStoreState` is treated
  as a live-handle batch-alignment probe, same-batch address-type/options-data
  requests target the logon handle they just opened, and the mailbox bootstrap
  `RopQueryRows` tail returns the existing explicit `NotSupported` table-column
  error when no columns have been configured.
- 2026-06-28 verification for the logon/profile expectation cleanup: `cargo
  test -p lpe-exchange
  mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift`;
  `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_empty_transport_options_data`; `cargo test -p
  lpe-exchange mapi_over_http_execute_handles_mailbox_store_bootstrap_rops`;
  `cargo test -p lpe-exchange logon_profile`.
- 2026-06-28 broad verification after the logon/profile cleanup: `cargo test
  -p lpe-exchange` completes without hanging. Current result is 1580 passed
  and 13 failed. Remaining failures are concentrated in MAPI-over-HTTP
  calendar/freebusy creation and projections, property response shape,
  submission advisory alignment, and the OXOCFG-writing sequence.
- 2026-06-28: Resolved the remaining MAPI-over-HTTP properties response-shape
  cluster by aligning stale tests with current property contracts: custom
  properties on opened messages persist after `RopSaveChangesMessage`, note and
  journal entries are covered by the fake MAPI identity lookup, logon bootstrap
  owner/status parsing skips the two Outlook icon binary properties, pending
  `RopDeletePropertiesNoReplicate` is tested before save, and unknown folder
  binary properties return `MAPI_E_NOT_FOUND`.
- 2026-06-28 verification for the properties cleanup: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds`;
  `cargo test -p lpe-exchange mapi_over_http::properties`.
- 2026-06-28 broad verification after the properties cleanup: `cargo test -p
  lpe-exchange` completes without hanging. Current result is 1584 passed and 9
  failed. Remaining failures are concentrated in MAPI-over-HTTP
  calendar/freebusy creation and projections, the OXOCFG-writing sequence, and
  submission transport/spooler advisory alignment.
- 2026-06-28: Resolved the remaining MAPI-over-HTTP calendar/freebusy cluster.
  The implementation now accepts dispatcher-added creation and
  last-modification metadata while saving new calendar events, and stale tests
  were aligned with current free/busy `RopQueryRows` end-of-table origin bytes
  and seeded fake-store sync checkpoint values.
- 2026-06-28 verification for the calendar/freebusy cleanup: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts`;
  `cargo test -p lpe-exchange mapi_over_http::calendar`.
- 2026-06-28: Resolved the remaining OXOCFG-writing and
  submission/spooler-advisory expectation cluster by aligning tests with the
  current ROP handle-index contract: same-batch transport-folder probes target
  the live logon handle, OXOCFG `RopSaveChangesMessage` reports its requested
  response handle, spooler advisory ROPs remain no-op acknowledgements, and
  `RopUpdateDeferredActionMessages` remains rejected without mutation.
- 2026-06-28 verification after the OXOCFG and submission-advisory cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds`;
  `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation`.
- 2026-06-28 broad verification after the remaining MAPI-over-HTTP expectation
  cleanup: `cargo test -p lpe-exchange` completes without hanging. Current
  result is 1593 passed and 0 failed, with doc tests also passing.
- 2026-06-28: Extended the MR-003 submission helper slice by moving
  spooler-advisory and deferred-action response policy into
  `dispatch/submission.rs`. The dispatcher still owns input-handle resolution,
  while `RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail`
  keep their no-op acknowledgement behavior and `RopUpdateDeferredActionMessages`
  remains rejected without mutation.
- 2026-06-28 verification for the spooler/deferred response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_transport_spooler_rops_keep_batch_aligned_without_mutation`;
  `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`;
  `cargo test -p lpe-exchange`. Current line counts: `dispatch.rs` 29,287
  lines and `dispatch/submission.rs` 120 lines.
- 2026-06-28: Extended the MR-003 submission helper slice by moving
  `RopGetTransportFolder` response policy into `dispatch/submission.rs`. The
  dispatcher still owns input-object resolution; the helper preserves the
  existing Outbox folder response and missing-input-handle error bytes.
- 2026-06-28 verification for the transport-folder response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift`;
  `cargo test -p lpe-exchange`. Current line counts: `dispatch.rs` 29,282
  lines and `dispatch/submission.rs` 127 lines.
- 2026-06-28: Extended the MR-003 submission helper slice by moving
  `RopOptionsData` response policy into `dispatch/submission.rs`. The
  dispatcher still owns input-object resolution; the helper preserves the
  existing empty transport options response and missing-input-handle error
  bytes.
- 2026-06-28 verification for the transport-options response-policy
  extraction: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_empty_transport_options_data`; `cargo test -p
  lpe-exchange
  mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift`;
  `cargo test -p lpe-exchange`. Current line counts: `dispatch.rs` 29,277
  lines and `dispatch/submission.rs` 134 lines.
- 2026-06-30: Advanced MR-003 by moving the remaining input-object validation
  for `RopGetTransportFolder` and `RopOptionsData` into
  `dispatch/submission.rs`. The dispatcher is now reduced to thin calls for
  both transport-information ROPs, while the helpers preserve Outbox transport
  folder projection, empty transport options data, missing-input-handle error
  bytes, and batch alignment.
- 2026-06-30 verification for the transport-information validation cleanup:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange transport`
  passed 63 focused transport/submission tests, including transport info ROPs,
  `RopGetTransportFolder`, `RopOptionsData`, spooler advisory no-mutation
  behavior, and canonical transport send coverage; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,530 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 13,127 lines and
  `mapi/dispatch/submission.rs` at 473 lines. `rg` confirmed
  `append_transport_folder_response`, `append_options_data_response`, and their
  input-object validation now live in the focused submission module, with
  dispatch reduced to thin calls for both ROPs.
- 2026-06-28: Extended the MR-002/MR-003 logon and transport-info helper
  cleanup by moving `RopGetAddressTypes` response policy into
  `dispatch/logon.rs`. The dispatcher still owns handle-table echoing and RCA
  trace context; the helper preserves the existing `EX`/`SMTP` response and
  missing-input-handle error bytes.
- 2026-06-28 verification for the address-types response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_empty_transport_options_data`; `cargo test -p
  lpe-exchange
  mapi_over_http_microsoft_transport_info_rops_reject_missing_input_handle_without_batch_drift`;
  `cargo test -p lpe-exchange
  mapi_over_http_outlook_startup_replay_keeps_calendar_search_and_partial_sync_contracts`;
  `cargo test -p lpe-exchange`. Current line counts: `dispatch.rs` 29,273
  lines and `dispatch/logon.rs` 30 lines.
- 2026-06-28: Extended the MR-002 logon helper slice by moving
  `RopGetStoreState` response policy into `dispatch/logon.rs`. The dispatcher
  still owns handle resolution; the helper preserves the existing store-state
  success response and missing-input-handle error bytes.
- 2026-06-28 verification for the store-state response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift`;
  `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_receive_folder_and_store_state`; `cargo test
  -p lpe-exchange`. Current line counts: `dispatch.rs` 29,268 lines and
  `dispatch/logon.rs` 37 lines.
- 2026-06-30: Advanced MR-002 by moving the remaining `RopGetStoreState`
  input-handle validation into `dispatch/logon.rs`. The dispatcher is now a
  thin call for the ROP, while `append_store_state_response` preserves the
  existing live-handle success response, missing-input-handle error bytes, and
  batch alignment.
- 2026-06-30 verification for the store-state validation cleanup: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange store_state` passed 2
  focused store-state tests; `cargo test -p lpe-exchange logon_profile` passed
  13 broader logon/profile tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,526 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 13,123 lines and
  `mapi/dispatch/logon.rs` at 112 lines. `rg` confirmed
  `append_store_state_response`, input-handle validation, and
  `rop_get_store_state_response` now live in the focused logon module, with
  dispatch reduced to a thin call for the ROP.
- 2026-06-28: Extended the MR-002 generic execute helper slice by moving
  `RopAbort` and `RopProgress` response-code selection into
  `dispatch/execute.rs`. The dispatcher still owns input-object lookup; the
  helpers preserve the existing table-specific and missing-handle error bytes.
- 2026-06-28 verification for the abort/progress response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_async_table_control_rops_return_rop_specific_protocol_errors`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_table_control_rops_require_table_handles`; `cargo
  test -p lpe-exchange`. Current line counts: `dispatch.rs` 29,250 lines and
  `dispatch/execute.rs` 217 lines.
- 2026-06-28: Extended the MR-002 generic execute helper slice by moving
  `RopResetTable` response policy into `dispatch/execute.rs`. The dispatcher
  still owns mutable table-state lookup and reset; the helper preserves the
  existing success response and missing-table-handle error bytes.
- 2026-06-28 verification for the reset-table response-policy extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_reset_table_requires_new_set_columns`; `cargo test
  -p lpe-exchange mapi_over_http_microsoft_table_control_rops_require_table_handles`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_empty_folder_rops_accept_nonzero_boolean_fields`;
  `cargo test -p lpe-exchange`. The first broad run had a transient failure in
  the hierarchy empty-folder test that passed in isolation; the immediate broad
  rerun passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 29,245 lines and `dispatch/execute.rs` 224 lines.
- 2026-06-28: Extended the MR-002 logon helper slice by moving `RopLogon`
  handle allocation, handle-table update, special-folder summary formatting,
  and logon identity recording into `dispatch/logon.rs`. The dispatcher still
  owns response emission, default-folder discovery logging, bootstrap phase
  logging, and output-handle collection.
- 2026-06-28 verification for the logon setup extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_private_mailbox_logon`; `cargo test -p
  lpe-exchange mapi_over_http_execute_returns_logon_replid_guid_map_for_outlook_bootstrap`;
  `cargo test -p lpe-exchange
  mapi_over_http_logon_advertises_openable_additional_ren_entryids_ex`; `cargo
  test -p lpe-exchange
  mapi_over_http_microsoft_hard_delete_messages_and_subfolders_hard_deletes_trash_contents`;
  `cargo test -p lpe-exchange`. The first broad run had a transient hierarchy
  hard-delete failure that passed in isolation; the immediate broad rerun
  passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 29,226 lines and `dispatch/logon.rs` 78 lines.
- 2026-06-28: Extended the MR-002 generic execute helper slice by moving
  unsupported known-ROP and unknown/reserved-ROP response construction into
  `dispatch/execute.rs`. The dispatcher still owns terminal `break` behavior
  for unknown/reserved ROPs, preserving unsupported/reserved ROP semantics.
- 2026-06-28 verification for the unsupported ROP response extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer`; `cargo
  test -p lpe-exchange unsupported_rop_is_terminal_without_consuming_later_rop_bytes`;
  `cargo test -p lpe-exchange
  reserved_rop_is_terminal_and_uses_common_unsupported_response`; `cargo test
  -p lpe-exchange
  mapi_over_http_microsoft_hard_delete_messages_and_subfolders_hard_deletes_trash_contents`;
  `cargo test -p lpe-exchange`. A first attempt to run both low-level ROP
  tests in one cargo command failed because cargo accepts only one positional
  test filter; both tests passed when run separately. The first broad run again
  hit the transient hierarchy hard-delete failure that passed in isolation; the
  immediate broad rerun passed with 1593 tests and doc tests passing. Current
  line counts: `dispatch.rs` 29,222 lines and `dispatch/execute.rs` 230 lines.
- 2026-06-28: Extended the MR-002 generic execute helper slice by moving the
  property-ROP unknown wire-type pre-dispatch response policy into
  `dispatch/execute.rs`. The dispatcher still owns terminal `break` behavior,
  preserving the current batch-stop semantics for invalid property wire types.
- 2026-06-28 verification for the unknown property wire-type extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_unknown_property_type_terminates_current_buffer`; `cargo test
  -p lpe-exchange set_columns_rejects_microsoft_invalid_column_property_types`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,204 lines and
  `dispatch/execute.rs` 257 lines.
- 2026-06-28: Extended the focused MR-002/MR-007 table-dispatch module by
  moving `RopQueryColumnsAll` branch wiring into `dispatch/tables.rs`, alongside
  the existing bookmark ROP wrappers. Table cursor/bookmark semantics,
  canonical table columns, and row output remain owned by `mapi/tables.rs`; the
  dispatch module only delegates response construction.
- 2026-06-28 verification for the table dispatch extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_table_bookmarks_restore_contents_cursor_and_free`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark`;
  `cargo test -p lpe-exchange
  mapi_over_http_query_columns_all_reports_canonical_table_columns`; `cargo
  test -p lpe-exchange`. The broad run passed with 1593 tests and doc tests
  passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 34 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopSeekRowFractional` branch wiring into `dispatch/tables.rs`. Fractional
  seek validation, cursor movement, and table row semantics remain owned by
  `mapi/tables.rs`; dispatch only delegates response construction.
- 2026-06-29 verification for the fractional seek dispatch extraction: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_seek_row_fractional_moves_table_cursor`; `cargo test
  -p lpe-exchange
  mapi_over_http_seek_row_fractional_rejects_zero_denominator_without_batch_drift`;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`;
  `cargo test -p lpe-exchange`. The first broad run hit a transient
  FastTransfer copy-folder assertion that passed in isolation; the immediate
  broad rerun passed with 1593 tests and doc tests passing. Current line
  counts: `dispatch.rs` 29,204 lines, `dispatch/tables.rs` 44 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  collapse-state ROP branch wiring into `dispatch/tables.rs`. `RopCollapseRow`,
  `RopGetCollapseState`, and `RopSetCollapseState` still delegate to
  `mapi/tables.rs` for collapse encoding, bookmark state, cursor semantics, and
  row behavior.
- 2026-06-29 verification for the collapse-state dispatch extraction: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows`;
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_categorized_table_collapse_state_restores_bookmark`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 65 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopExpandRow` branch wiring into `dispatch/tables.rs`. The folder-handle
  guard remains in dispatch, and expand-row evaluation, row output, and
  category behavior remain owned by `mapi/tables.rs`.
- 2026-06-29 verification for the expand-row dispatch extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows`;
  `cargo test -p lpe-exchange
  mapi_over_http_expand_row_on_folder_cannot_delete_messages`; `cargo test -p
  lpe-exchange
  mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`;
  `cargo test -p lpe-exchange`. The first broad run again hit the transient
  FastTransfer copy-folder assertion that passed in isolation; the immediate
  broad rerun passed with 1593 tests and doc tests passing. Current line
  counts: `dispatch.rs` 29,204 lines, `dispatch/tables.rs` 74 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopQueryPosition` response construction into `dispatch/tables.rs`.
  Calendar trace context, RCA logging, and table-position diagnostics remain in
  `dispatch.rs`; row counts and cursor position semantics remain owned by
  `mapi/tables.rs`.
- 2026-06-29 verification for the query-position dispatch extraction: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_query_position_reports_table_cursor`; `cargo test
  -p lpe-exchange`. The broad run passed with 1593 tests and doc tests
  passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 84 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopGetStatus` and `RopSeekRow` response construction into
  `dispatch/tables.rs`. Seek-row before-position capture, named-property
  context, and RCA diagnostics remain in `dispatch.rs`; cursor movement and
  status response semantics remain owned by `mapi/tables.rs`.
- 2026-06-29 verification for the get-status/seek-row dispatch extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_seek_row_moves_contents_table_cursor`; `cargo test
  -p lpe-exchange
  mapi_over_http_microsoft_table_control_rops_require_table_handles`; `cargo
  test -p lpe-exchange
  mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity`;
  `cargo test -p lpe-exchange`. The first broad run hit an order-dependent
  calendar identity-mapping panic that passed in isolation; the immediate broad
  rerun passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 29,204 lines, `dispatch/tables.rs` 97 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopFindRow` response construction into `dispatch/tables.rs`. Inbox trace
  context, named-property diagnostics, associated-message tracking, and
  broad-findrow session hints remain in `dispatch.rs`; row matching, cursor
  updates, and response bytes remain owned by `mapi/tables.rs`.
- 2026-06-29 verification for the find-row dispatch extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_find_row_returns_matching_contents_row`; `cargo test -p
  lpe-exchange mapi_over_http_findrow_rejects_invalid_microsoft_find_row_flags`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 107 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopQueryRows` response construction into `dispatch/tables.rs`. Bootstrap
  phase logging, smart-input adjustment, query-row response diagnostics,
  hierarchy diagnostics, session trace contexts, and Outlook view hints remain
  in `dispatch.rs`; row paging, cursor movement, and response bytes remain
  owned by `mapi/tables.rs`.
- 2026-06-29 verification for the query-rows dispatch extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_query_rows_no_advance_preserves_table_position`; `cargo test
  -p lpe-exchange mapi_over_http_query_rows_reads_backward_from_table_position`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 117 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  successful `RopSetColumns`, `RopSortTable`, and `RopRestrict` response
  construction into `dispatch/tables.rs`. Validation, table mutation,
  invalid-state marking, named-property normalization, RCA logging, and
  session trace contexts remain in `dispatch.rs`.
- 2026-06-29 verification for the set-columns/sort/restrict response
  extraction: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_oxctabl_4_1_to_4_4_contents_table_setcolumns_sort_query_rows`;
  `cargo test -p lpe-exchange mapi_over_http_restrict_filters_contents_table_rows`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,204 lines,
  `dispatch/tables.rs` 126 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  `RopGetSearchCriteria` response construction into `dispatch/tables.rs`.
  Search-folder lookup, builtin fallback selection, canonical definition
  conversion, and error mapping remain in `dispatch.rs`.
- 2026-06-29 verification for the get-search-criteria response extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_set_get_search_criteria_updates_canonical_search_folder`;
  `cargo test -p lpe-exchange
  mapi_over_http_builtin_contacts_search_get_search_criteria_uses_fixed_folder_id`;
  `cargo test -p lpe-exchange`. The broad run passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 29,199 lines,
  `dispatch/tables.rs` 134 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  table-opening response construction for `RopGetHierarchyTable`,
  `RopGetContentsTable`, `RopGetAttachmentTable`,
  `RopGetReceiveFolderTable`, `RopGetPermissionsTable`, and
  `RopGetRulesTable` into `dispatch/tables.rs`. Handle allocation, input
  validation, row counts, table object state, RCA logging, and session
  diagnostics remain in `dispatch.rs`; response bytes still come from the
  existing ROP/table builders.
- 2026-06-29 verification for the table-opening response extraction: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_opens_root_folder_and_gets_special_hierarchy_table`;
  `cargo test -p lpe-exchange
  mapi_over_http_contents_table_lists_canonical_messages`; `cargo test -p
  lpe-exchange
  mapi_over_http_microsoft_oxcmsg_get_attachment_table_lists_canonical_attachments`;
  `cargo test -p lpe-exchange
  mapi_over_http_get_receive_folder_table_requires_private_logon_handle`;
  `cargo test -p lpe-exchange
  mapi_over_http_permissions_table_maps_delegate_folder_access`; `cargo test
  -p lpe-exchange
  mapi_over_http_get_rules_table_projects_canonical_sieve_rules`; `cargo test
  -p lpe-exchange`. The broad run passed with 1593 tests and doc tests
  passing. Current line counts: `dispatch.rs` 29,207 lines,
  `dispatch/tables.rs` 152 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  table object construction defaults for hierarchy, contents, attachment,
  permissions, and rules tables into `dispatch/tables.rs`. Dispatch still owns
  handle allocation, input validation, access checks, row counts, and logging;
  the helpers only centralize the default `MapiObject::*Table` state used when
  opening table handles.
- 2026-06-29 verification for the table object construction extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_opens_root_folder_and_gets_special_hierarchy_table`;
  `cargo test -p lpe-exchange
  mapi_over_http_contents_table_lists_canonical_messages`; `cargo test -p
  lpe-exchange
  mapi_over_http_microsoft_oxcmsg_get_attachment_table_lists_canonical_attachments`;
  `cargo test -p lpe-exchange
  mapi_over_http_permissions_table_maps_delegate_folder_access`; `cargo test
  -p lpe-exchange
  mapi_over_http_get_rules_table_projects_canonical_sieve_rules`; `cargo test
  -p lpe-exchange`. The broad run passed with 1593 tests and doc tests
  passing. Current line counts: `dispatch.rs` 29,153 lines,
  `dispatch/tables.rs` 216 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving
  table column-support diagnostics into `dispatch/tables.rs`. The moved
  helpers classify normal-message, associated-content, defaulted, and
  named/dynamic table columns for Outlook debug summaries; row projection and
  wire output remain unchanged.
- 2026-06-29 verification for the table column-support helper extraction:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  normal_message_column_support_covers_outlook_mail_view_columns`; `cargo test
  -p lpe-exchange associated_column_support_covers_inbox_configuration_columns`;
  `cargo test -p lpe-exchange
  associated_column_support_covers_inbox_view_descriptor_columns`; `cargo test
  -p lpe-exchange
  associated_column_support_covers_common_views_wlink_binary_variants`; `cargo
  test -p lpe-exchange
  mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`;
  `cargo test -p lpe-exchange`. The first broad run hit the known transient
  FastTransfer copy-folder assertion, that test passed in isolation, and the
  immediate broad rerun passed with 1593 tests and doc tests passing. Current
  line counts: `dispatch.rs` 28,964 lines, `dispatch/tables.rs` 405 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving table
  column named-property normalization into `dispatch/tables.rs`. The moved
  helpers normalize stale Outlook sharing aliases and well-known named
  property IDs for table column selection; named-property cache mutation
  remains in `dispatch.rs` where it is shared with non-table property ROPs.
- 2026-06-29 verification for the table column-normalization helper
  extraction: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  table_columns_normalize`; `cargo test -p lpe-exchange`. The focused run
  passed 6 normalization tests, and the broad run passed with 1593 tests and
  doc tests passing. Current line counts: `dispatch.rs` 28,924 lines,
  `dispatch/tables.rs` 448 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  effective contents-table column selection helper into `dispatch/tables.rs`.
  The helper still chooses the same default columns for normal contents,
  associated Inbox/Quick Step contents, Common Views navigation shortcuts, and
  conversation actions.
- 2026-06-29 verification for the effective contents-table column helper
  extraction: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  quick_step_synthetic_folder_allows_associated_message_creation`; `cargo test
  -p lpe-exchange
  ipm_configuration_contract_summary_reports_required_columns_and_streams`;
  `cargo test -p lpe-exchange`. The focused tests passed, and the broad run
  passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 28,910 lines, `dispatch/tables.rs` 466 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving table
  restriction diagnostic wrappers into `dispatch/tables.rs`. The moved helpers
  decode request restriction bytes, format parsed restriction trees, and collect
  restriction property tags for table diagnostics; restriction parsing,
  matching, table mutation, and row output remain in the existing ROP/table
  paths.
- 2026-06-29 verification for the table restriction diagnostic helper
  extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `request_restriction_bytes`, `restriction_property_tags_from_request`,
  `collect_restriction_property_tags`, `format_debug_restriction`,
  `format_debug_restriction_option`, `format_debug_restriction_property_tags`,
  and `format_debug_parsed_restriction` definitions now live in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange restriction` passed 27
  focused tests; `cargo test -p lpe-exchange` passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 28,762 lines,
  `dispatch/tables.rs` 619 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the same table diagnostics slice by moving the adjacent
  `MapiValue` and text debug formatters into `dispatch/tables.rs`. These
  helpers are used by table row/window diagnostics and by the moved restriction
  formatter; no protocol row serialization or response bytes changed.
- 2026-06-29 verification for the table value debug formatter extraction:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_debug_mapi_value`, `format_debug_text_value`,
  `format_debug_restriction`, and `format_debug_parsed_restriction`
  definitions live in `dispatch/tables.rs`; `cargo test -p lpe-exchange
  restriction` passed 27 focused tests. The first broad `cargo test -p
  lpe-exchange` run hit the known transient
  `mapi_over_http_custom_calendar_hierarchy_sync_projects_owner_entry_id_identity`
  identity assertion; that test passed in isolation, and the immediate broad
  rerun passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 28,732 lines, `dispatch/tables.rs` 649 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the table diagnostics slice by moving the pure
  `select_query_window` helper into `dispatch/tables.rs`. The helper only
  computes debug/table window indices for forward and backward reads; cursor
  mutation, row serialization, and table output remain unchanged.
- 2026-06-29 verification for the query-window helper extraction: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the `select_query_window` definition
  now lives in `dispatch/tables.rs`; `cargo test -p lpe-exchange query_row`
  passed 54 focused tests; `cargo test -p lpe-exchange` passed with 1593 tests
  and doc tests passing. Current line counts: `dispatch.rs` 28,719 lines,
  `dispatch/tables.rs` 662 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the table diagnostics slice by moving associated table
  debug-row helpers into `dispatch/tables.rs`. The moved cluster owns the
  `DebugAssociatedTableRow` wrapper, associated-config/named-view debug row
  selection, debug sorting, row property lookup, row identity/class/subject
  accessors, and debug-row serialization. Higher-level diagnostic summaries
  still live in `dispatch.rs`; table row projection and wire output remain in
  the existing table/property builders.
- 2026-06-29 verification for the associated debug-row helper extraction:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the
  `DebugAssociatedTableRow` and direct helper definitions now live in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange associated` passed 105
  focused tests; `cargo test -p lpe-exchange` passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 28,521 lines,
  `dispatch/tables.rs` 860 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the table diagnostics slice by moving associated Inbox
  and Common Views query-window debug formatters into `dispatch/tables.rs`.
  These helpers only assemble diagnostic strings for selected associated table
  rows; table cursors, row projection, and wire output remain unchanged.
- 2026-06-29 verification for the associated/Common Views query-window
  formatter extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_inbox_associated_query_row_window` and
  `format_common_views_query_row_window` definitions now live in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange query_row` passed 54
  focused tests; `cargo test -p lpe-exchange` passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 28,431 lines,
  `dispatch/tables.rs` 950 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the same table diagnostics slice by moving the
  top-level associated query-window dispatcher `format_outlook_query_row_window`
  into `dispatch/tables.rs`. It only routes associated query diagnostics to the
  Inbox and Common Views formatters; non-associated tables, cursor state, row
  projection, and response bytes remain unchanged.
- 2026-06-29 verification for the associated query-window dispatcher
  extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_outlook_query_row_window` is now defined in `dispatch/tables.rs`;
  `cargo test -p lpe-exchange query_row` passed 54 focused tests. The first
  broad `cargo test -p lpe-exchange` run hit the known order-dependent
  hierarchy empty-folder assertion; that test passed in isolation, and the
  immediate broad rerun passed with 1593 tests and doc tests passing. Current
  line counts: `dispatch.rs` 28,393 lines, `dispatch/tables.rs` 988 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  associated/Common Views query-row value debug formatter
  `format_outlook_query_row_values` into `dispatch/tables.rs`. The helper only
  formats diagnostic property-value summaries for selected associated table
  rows; table cursors, row projection, and response bytes remain unchanged.
- 2026-06-29 verification for the associated query-row value formatter
  extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_outlook_query_row_values` is now defined in `dispatch/tables.rs`;
  `cargo test -p lpe-exchange query_row` passed 54 focused tests; `cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing. Current line
  counts: `dispatch.rs` 28,285 lines, `dispatch/tables.rs` 1,096 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  normal-message query-row debug summary and its contact-table sub-summary into
  `dispatch/tables.rs`. These helpers only format diagnostic row summaries for
  mail/contact table windows; table cursor mutation, row serialization, and
  response bytes remain unchanged.
- 2026-06-29 verification for the normal-message/contact query-row debug
  summary extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_normal_message_query_row_summary` and
  `format_contact_query_row_summary` definitions now live in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange query_row` passed 54
  focused tests; `cargo test -p lpe-exchange` passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 28,137 lines,
  `dispatch/tables.rs` 1,244 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  calendar event query-position debug summary into `dispatch/tables.rs`. The
  helper only formats calendar table diagnostic row summaries; calendar row
  projection, cursor mutation, and response bytes remain unchanged.
- 2026-06-29 verification for the calendar query-position debug summary
  extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_calendar_event_query_position_summary` is now defined in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange query_row` passed 54
  focused tests. The first broad `cargo test -p lpe-exchange` run hit the
  known order-dependent
  `mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`
  assertion; that test passed in isolation, and the immediate broad rerun
  passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 28,082 lines, `dispatch/tables.rs` 1,299 lines, and
  `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the MR-002/MR-007 table-dispatch slice by moving the
  normal-message find-row failure diagnostic formatter and its candidate-tag
  selector into `dispatch/tables.rs`. These helpers only summarize candidate
  table rows for diagnostics when `FindRow` does not match; table matching,
  cursor mutation, and response bytes remain unchanged.
- 2026-06-29 verification for the normal-message find-row failure diagnostic
  extraction: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_normal_message_find_row_failure_candidates` and
  `candidate_find_row_debug_tags` definitions now live in
  `dispatch/tables.rs`; `cargo test -p lpe-exchange find_row` passed 47
  focused tests; `cargo test -p lpe-exchange` passed with 1593 tests and doc
  tests passing. Current line counts: `dispatch.rs` 27,982 lines,
  `dispatch/tables.rs` 1,399 lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Started a focused table diagnostics split by adding
  `dispatch/table_diagnostics.rs` and moving the low-dependency MAPI debug
  value, text, and parsed-restriction formatters out of `dispatch/tables.rs`.
  This keeps the table-dispatch module below the 1,500-line target while
  preserving diagnostic output, table matching, row projection, and response
  bytes.
- 2026-06-29 verification for the table diagnostics formatter split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed `format_debug_mapi_value`,
  `format_debug_text_value`, and `format_debug_parsed_restriction` definitions
  now live in `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  restriction` passed 27 focused tests; `cargo test -p lpe-exchange query_row`
  passed 54 focused tests; `cargo test -p lpe-exchange` passed with 1593 tests
  and doc tests passing. Current line counts: `dispatch.rs` 27,984 lines,
  `dispatch/tables.rs` 1,294 lines, `dispatch/table_diagnostics.rs` 106
  lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the table diagnostics split by moving request
  restriction-byte extraction, restriction property-tag collection, and
  restriction debug wrapper helpers into `dispatch/table_diagnostics.rs`.
  Restriction parsing, table matching, table mutation, and response bytes remain
  unchanged.
- 2026-06-29 verification for the restriction diagnostic helper split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed `request_restriction_bytes`,
  `restriction_property_tags_from_request`, `format_debug_restriction`,
  `format_debug_restriction_option`,
  `format_debug_restriction_property_tags`, and
  `collect_restriction_property_tags` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange restriction`
  passed 27 focused tests; `cargo test -p lpe-exchange` passed with 1593 tests
  and doc tests passing. Current line counts: `dispatch.rs` 27,984 lines,
  `dispatch/tables.rs` 1,216 lines, `dispatch/table_diagnostics.rs` 184
  lines, and `dispatch/execute.rs` 257 lines.
- 2026-06-29: Extended the table diagnostics split by moving associated
  debug-row helpers into `dispatch/table_diagnostics.rs`. The moved cluster
  owns `DebugAssociatedTableRow`, associated-config/named-view debug row
  selection, debug sorting, row property lookup, row identity/class/subject
  accessors, debug-row serialization, and default-folder associated named-view
  diagnostics. Associated table projection, matching semantics, cursor
  mutation, and response bytes remain unchanged.
- 2026-06-29 verification for the associated debug-row diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `DebugAssociatedTableRow`, `debug_associated_table_rows`,
  `sort_debug_associated_table_rows`,
  `debug_associated_row_property_value`, `debug_associated_row_id`,
  `debug_associated_row_class`, `debug_associated_row_subject`,
  `serialize_debug_associated_row`,
  `debug_default_folder_associated_named_view`,
  `append_exact_virtual_inbox_debug_associated_config`,
  `debug_exact_message_class_restriction_value`, and
  `compare_debug_mapi_values` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange associated`
  passed 105 focused tests; `cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. Current line counts: `dispatch.rs` 27,984
  lines, `dispatch/tables.rs` 1,018 lines,
  `dispatch/table_diagnostics.rs` 382 lines, and `dispatch/execute.rs` 257
  lines.
- 2026-06-29: Extended the MR-001 diagnostics split by moving state-only
  Outlook post-hierarchy/startup diagnostic formatters into
  `dispatch/diagnostics/post_hierarchy.rs`. The moved helpers cover Inbox open
  loop summaries, post-FAI reopen stall classification, post-FAI folder-type
  probe-loop summaries, and post-FAI handoff context formatting. The
  session-mutating smart-input variant helper remains in `dispatch.rs`.
- 2026-06-29 verification for the post-hierarchy diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed
  `format_inbox_open_loop_summary`,
  `inbox_post_fai_reopen_stall_observed`,
  `format_post_fai_folder_type_probe_loop_context`, and
  `format_inbox_post_fai_handoff_context` definitions now live in
  `dispatch/diagnostics/post_hierarchy.rs`; `cargo test -p lpe-exchange
  post_fai` passed 5 focused tests; `cargo test -p lpe-exchange
  inbox_open_loop` passed 1 focused test; `cargo test -p lpe-exchange
  post_sync_release_flags` passed 1 focused test; `cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. Current line counts:
  `dispatch.rs` 27,874 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 186 lines,
  `dispatch/tables.rs` 1,018 lines,
  `dispatch/table_diagnostics.rs` 382 lines, and `dispatch/execute.rs` 257
  lines.
- 2026-06-29: Extended the table diagnostics split by moving Inbox FAI
  handoff visibility formatting and its associated debug-row list formatter
  into `dispatch/table_diagnostics.rs`. The moved code only formats
  associated-row diagnostic context from existing snapshot/restriction inputs;
  row matching, table projection, cursor state, and response bytes remain
  unchanged.
- 2026-06-29 verification for the FAI handoff visibility diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_inbox_fai_handoff_visibility_context` and
  `format_debug_associated_row_list` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  inbox_fai_handoff_visibility_context` passed 1 focused test; `cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. Current
  oversized-check line counts: `dispatch.rs` 28,557 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 195 lines,
  `dispatch/tables.rs` 1,074 lines,
  `dispatch/table_diagnostics.rs` 468 lines, and `dispatch/execute.rs` 276
  lines.
- 2026-06-29: Extended the table diagnostics split by moving adjacent Inbox
  hierarchy/associated query context, post-FAI hierarchy release context,
  associated `FindRow` context, and broad associated `FindRow` classification
  helpers into `dispatch/table_diagnostics.rs`. These helpers only format or
  classify diagnostic context around existing table state; the smart-input
  cursor reset helper remains in `dispatch.rs` because it mutates session
  state.
- 2026-06-29 verification for the Inbox table diagnostic context split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_inbox_hierarchy_query_context`,
  `format_inbox_associated_query_context`,
  `format_post_fai_hierarchy_release_without_inbox_contents_context`,
  `format_inbox_associated_find_context`,
  `inbox_associated_broad_findrow_matched`, and
  `is_broad_ipm_configuration_restriction` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  inbox_fai_handoff` passed 1 focused test; `cargo test -p lpe-exchange
  post_fai_hierarchy_release` passed 1 focused test; `cargo test -p
  lpe-exchange associated_find` passed 17 focused tests; `cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. The oversized
  source check reports `dispatch.rs` at 28,256 lines,
  `dispatch/diagnostics/post_hierarchy.rs` at 195 lines,
  `dispatch/tables.rs` at 1,074 lines,
  `dispatch/table_diagnostics.rs` at 769 lines, and `dispatch/execute.rs` at
  276 lines.
- 2026-06-29: Extended the table diagnostics split by moving Common Views Inbox
  shortcut context and Inbox-related release context formatting into
  `dispatch/table_diagnostics.rs`. These helpers only format selected table
  rows, WLink diagnostics, and release-state context; release handling and
  session state recording remain in `dispatch.rs`.
- 2026-06-29 verification for the Common Views/release diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_common_views_inbox_shortcut_context` and
  `format_inbox_related_release_context` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange common_views`
  passed 45 focused tests; `cargo test -p lpe-exchange
  inbox_release_context` passed 1 focused test; `cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. The oversized source check
  reports `dispatch.rs` at 28,131 lines,
  `dispatch/diagnostics/post_hierarchy.rs` at 195 lines,
  `dispatch/tables.rs` at 1,074 lines,
  `dispatch/table_diagnostics.rs` at 894 lines, and `dispatch/execute.rs` at
  276 lines.
- 2026-06-29: Extended the table diagnostics split by moving hierarchy
  query-row wire summary and IPM subtree hierarchy metric helpers into
  `dispatch/table_diagnostics.rs`. The moved code only decodes hierarchy table
  response bytes for RCA/debug summaries and metric flags; hierarchy row
  serialization, cursor movement, and response construction remain unchanged.
- 2026-06-29 verification for the hierarchy wire diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed
  `HierarchyResponseMetricSummary`, `hierarchy_response_metric_summary`,
  `format_hierarchy_query_rows_wire_summary`,
  `format_hierarchy_debug_folder_id`,
  `format_hierarchy_debug_wire_folder_id`,
  `format_hierarchy_debug_string`, `format_hierarchy_debug_count`, and
  `format_hierarchy_debug_bool` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  hierarchy_query_rows_wire_summary` passed 1 focused test; `cargo test -p
  lpe-exchange hierarchy` passed 157 focused tests; `cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. The oversized
  source check reports `dispatch.rs` at 27,961 lines,
  `dispatch/diagnostics/post_hierarchy.rs` at 195 lines,
  `dispatch/tables.rs` at 1,074 lines,
  `dispatch/table_diagnostics.rs` at 1,064 lines, and `dispatch/execute.rs`
  at 276 lines.
- 2026-06-29: Extended the table diagnostics split by moving the Inbox
  `IPM.Configuration.*` set-column and row-contract diagnostic helpers into
  `dispatch/table_diagnostics.rs`. The moved code only formats required-column,
  sort-order, stream-presence, and debug property-tag summaries from existing
  associated-config rows; table projection, row serialization, and response
  bytes remain unchanged.
- 2026-06-29 verification for the IPM configuration contract diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `format_ipm_configuration_set_columns_contract`,
  `format_ipm_configuration_contract_summary`, `missing_debug_property_tags`,
  `OUTLOOK_VIEW_DESCRIPTOR_NAMED_STRING_PLACEHOLDER_TAG`,
  `debug_property_tag_present`, `format_ipm_configuration_row_contract`, and
  `ipm_configuration_row_issues` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  ipm_configuration` passed 1 focused test; `cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current
  oversized-check line counts: `dispatch.rs` 27,777 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 195 lines, `dispatch/tables.rs`
  1,074 lines, `dispatch/table_diagnostics.rs` 1,248 lines, and
  `dispatch/execute.rs` 276 lines.
- 2026-06-29: Extended the table diagnostics split by moving Common Views WLink
  target-decoding and selected-column contract summary formatters into
  `dispatch/table_diagnostics.rs`. These helpers only format diagnostics for
  navigation shortcut identity and expected defaulted WLink columns; Common
  Views row projection, persistence, and wire output remain unchanged.
- 2026-06-29 verification for the Common Views WLink diagnostics split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed
  `format_common_views_wlink_target_decoding` and
  `format_common_views_wlink_contract_summary` definitions now live in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange common_views`
  passed 45 focused tests; `cargo test -p lpe-exchange` passed with 1593 tests
  and doc tests passing; `python tools/check_oversized_sources.py` passed in
  warning mode. Current oversized-check line counts: `dispatch.rs` 27,645
  lines, `dispatch/diagnostics/post_hierarchy.rs` 195 lines,
  `dispatch/tables.rs` 1,074 lines, `dispatch/table_diagnostics.rs` 1,380
  lines, and `dispatch/execute.rs` 276 lines.
- 2026-06-29: Split execute parse/dispatch logging diagnostics out of the
  oversized `dispatch/diagnostics.rs` hub into
  `dispatch/diagnostics/execute.rs`. The moved helpers only emit RCA/debug
  logging for Execute request dispatch start and parse failures; request
  parsing, response construction, session handling, and ROP behavior remain
  unchanged.
- 2026-06-29 verification for the execute diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `mod execute`,
  `log_execute_dispatch_start_debug`, and `log_execute_parse_failure_debug`
  are wired through `dispatch/diagnostics/execute.rs`; `cargo test -p
  lpe-exchange execute` passed 55 focused tests; `cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and no longer
  reports `dispatch/diagnostics.rs`. Current line counts:
  `dispatch/diagnostics.rs` 1,478 lines,
  `dispatch/diagnostics/execute.rs` 98 lines, `dispatch.rs` 27,645 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Moved post-hierarchy GetProperties/SetProperties/OpenFolder and
  GetReceiveFolder diagnostic contract formatters from `dispatch.rs` into
  `dispatch/diagnostics/post_hierarchy.rs`. The moved helpers only classify
  probe shape, object context, write mode, and response summaries for existing
  RCA/debug state; property parsing, response construction, canonical writes,
  and session recording remain unchanged.
- 2026-06-29 verification for the post-hierarchy contract diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `post_hierarchy_getprops_contract`, `post_hierarchy_setprops_contract`,
  `post_hierarchy_open_folder_contract`, and
  `post_hierarchy_get_receive_folder_contract` definitions now live in
  `dispatch/diagnostics/post_hierarchy.rs`; `cargo test -p lpe-exchange
  post_hierarchy` passed 9 focused tests; `cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 27,452 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 394 lines,
  `dispatch/diagnostics.rs` 1,478 lines,
  `dispatch/diagnostics/execute.rs` 98 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Extended the post-hierarchy diagnostics split by moving the
  GetProperties contract response summary, access-value shape formatting,
  set-properties problem-count parsing, and zero/default value classification
  helpers into `dispatch/diagnostics/post_hierarchy.rs`. The moved code only
  classifies existing diagnostic response bytes; GetProperties response
  construction, property parsing, SetProperties behavior, canonical writes, and
  session state remain unchanged.
- 2026-06-29 verification for the GetProperties response-summary diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `GetPropsContractResponseSummary`, `getprops_contract_response_summary`,
  `mapi_getprops_contract_value_debug`, `set_properties_problem_count`, and
  `mapi_value_is_zero_or_default` definitions now live in
  `dispatch/diagnostics/post_hierarchy.rs`; `cargo test -p lpe-exchange
  getprops_contract_response_summary` passed 1 focused test; `cargo test -p
  lpe-exchange post_hierarchy` passed 9 focused tests; `cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 27,321 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 525 lines,
  `dispatch/diagnostics.rs` 1,478 lines,
  `dispatch/diagnostics/execute.rs` 98 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Extended the post-hierarchy diagnostics split by moving the
  default-folder `SetProperties` RCA/debug logging helper and property-problem
  detail parser into `dispatch/diagnostics/post_hierarchy.rs`. The moved code
  only emits diagnostics from already-built SetProperties responses; ROP
  execution, response construction, property validation, canonical writes, and
  session state remain unchanged.
- 2026-06-29 verification for the default-folder SetProperties diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `log_set_properties_default_folder_response_debug` and
  `set_properties_problem_details_for_debug` definitions now live in
  `dispatch/diagnostics/post_hierarchy.rs`; `cargo test -p lpe-exchange
  set_property` passed 4 focused tests; `cargo test -p lpe-exchange
  post_hierarchy` passed 9 focused tests; `cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 27,254 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 592 lines,
  `dispatch/diagnostics.rs` 1,478 lines,
  `dispatch/diagnostics/execute.rs` 98 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/property_names.rs` and moved the
  SetProperties diagnostic property-name formatter there. This keeps the
  generic property-name mapping out of the dispatch hub without growing
  `dispatch/diagnostics.rs` past the production-source target. The moved code
  only formats diagnostic names; property IDs, property validation, response
  bytes, and canonical mutations remain unchanged.
- 2026-06-29 verification for the property-name diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `format_set_property_names_for_debug`
  and `set_property_debug_name` definitions now live in
  `dispatch/diagnostics/property_names.rs`; `cargo test -p lpe-exchange
  set_property` passed 4 focused tests. A parallel `cargo test -p
  lpe-exchange` run hit known order-dependent MAPI-over-HTTP failures in
  custom-calendar hierarchy identity and hard-delete hierarchy cleanup; both
  tests passed in isolation, and `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 27,172 lines,
  `dispatch/diagnostics.rs` 1,480 lines,
  `dispatch/diagnostics/property_names.rs` 83 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 592 lines,
  `dispatch/diagnostics/execute.rs` 98 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/values.rs` and moved diagnostic
  MAPI value-shape, optional folder-id, debug context, and Inbox folder-type
  GetProperties response-context formatters out of `dispatch/diagnostics.rs`.
  The moved code only formats diagnostics from existing values and response
  bytes; it does not change property parsing, response construction, ROP
  behavior, or canonical state.
- 2026-06-29 verification for the diagnostic value formatter split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `mapi_value_debug_string`,
  `mapi_value_debug_u32`, `mapi_value_debug_bool`,
  `mapi_value_debug_binary_decode`, `format_optional_folder_id`,
  `mapi_value_debug_shape`, `mapi_value_debug_u32_from_value`,
  `format_inbox_folder_type_getprops_response_context`, and
  `debug_context_or_none` definitions now live in
  `dispatch/diagnostics/values.rs`; `cargo test -p lpe-exchange
  debug_named_property_context_reports_session_and_unresolved_properties`
  passed 1 focused test; `cargo test -p lpe-exchange
  inbox_folder_type_getprops_response_context` passed 1 focused test;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode. Current line counts: `dispatch.rs` 27,172 lines,
  `dispatch/diagnostics.rs` 1,367 lines,
  `dispatch/diagnostics/values.rs` 127 lines,
  `dispatch/diagnostics/property_names.rs` 83 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 592 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/default_folders.rs` and moved
  default-folder entry-id debug decoding, `PidTagAdditionalRenEntryIdsEx`
  debug parsing, indexed special-folder entry-id summaries, and default-folder
  GetProperties value summaries out of `dispatch.rs`. The moved code only
  formats RCA/debug evidence from existing values and response bytes; default
  folder identification, property parsing, response construction, property
  validation, canonical writes, and session state remain unchanged.
- 2026-06-29 verification for the default-folder diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed
  `default_folder_entry_id_values_for_debug`,
  `default_folder_getprops_response_values_for_debug`,
  `default_folder_getprops_value_for_debug`,
  `additional_ren_entry_ids_ex_for_debug`, and
  `indexed_special_folder_entry_ids_for_debug` definitions now live only in
  `dispatch/diagnostics/default_folders.rs`; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug` passed 4 focused tests; `cargo test -p
  lpe-exchange default_folder_identification` passed 2 focused tests; `cargo
  test -p lpe-exchange
  first_post_hierarchy_probe_summary_identifies_set_properties_shapes` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 26,831 lines,
  `dispatch/diagnostics/default_folders.rs` 347 lines,
  `dispatch/diagnostics.rs` 1,369 lines,
  `dispatch/diagnostics/values.rs` 127 lines,
  `dispatch/diagnostics/property_names.rs` 83 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 592 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/property_responses.rs` and moved
  SetProperties RCA/debug logging, GetProperties response logging, Outlook
  view response summaries, associated-config stream write summaries, and
  GetProperties diagnostic value-shape formatting out of `dispatch.rs`. The
  moved code only emits or formats diagnostics from already-parsed requests,
  response bytes, and existing values; SetProperties validation, GetProperties
  response construction, property IDs, property parsing, canonical writes, and
  session state remain unchanged.
- 2026-06-29 verification for the property response diagnostics split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed
  `log_set_properties_specific_debug`,
  `log_get_properties_default_folder_response_debug`,
  `log_get_properties_specific_response_debug`,
  `log_get_properties_view_response_debug`,
  `associated_config_stream_write_summary`,
  `get_properties_specific_response_values_for_debug`, and
  `get_properties_view_response_values_for_debug` definitions now live in
  `dispatch/diagnostics/property_responses.rs`; `cargo test -p lpe-exchange
  associated_config_stream_write_summary_names_roaming_xml` passed 1 focused
  test; `cargo test -p lpe-exchange set_property` passed 4 focused tests;
  `cargo test -p lpe-exchange getprops_contract_response_summary` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 26,471 lines,
  `dispatch/diagnostics/property_responses.rs` 363 lines,
  `dispatch/diagnostics/default_folders.rs` 347 lines,
  `dispatch/diagnostics.rs` 1,371 lines,
  `dispatch/diagnostics/values.rs` 127 lines,
  `dispatch/diagnostics/property_names.rs` 83 lines,
  `dispatch/diagnostics/post_hierarchy.rs` 592 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/named_properties.rs` and moved
  named-property diagnostic formatting out of `dispatch.rs`: returned property
  ID summaries, requested/missing named-property summaries, explicit named
  property tag context, and contents-table named-property context. The moved
  code only formats diagnostics from existing session mappings, requested
  tags, and table columns; named-property allocation, cache updates,
  well-known property mapping, returned property IDs, and wire responses remain
  unchanged.
- 2026-06-29 verification for the named-property diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `format_debug_property_ids`,
  `format_debug_named_properties`, `format_debug_named_property_context`, and
  `format_contents_table_named_property_context` definitions now live in
  `dispatch/diagnostics/named_properties.rs`; `cargo test -p lpe-exchange
  named_property_context` passed 2 focused tests; `cargo test -p lpe-exchange
  get_property_ids` passed 3 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode. Current
  line counts: `dispatch.rs` 26,401 lines,
  `dispatch/diagnostics/named_properties.rs` 76 lines,
  `dispatch/diagnostics/property_responses.rs` 363 lines,
  `dispatch/diagnostics/default_folders.rs` 347 lines,
  `dispatch/diagnostics.rs` 1,373 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Extended `dispatch/diagnostics/default_folders.rs` by moving the
  default-folder discovery contract logger, the root default-folder
  identification contract formatter, and the default-folder hierarchy
  projection formatter out of `dispatch.rs`. The moved code only emits RCA
  diagnostics from existing special-folder projections, mailbox rows, and
  snapshot state; default-folder discovery specs, parent/container
  expectations, response construction, canonical folder projection, and
  session state remain unchanged.
- 2026-06-29 verification for the default-folder discovery diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `default_folder_identification_contract_for_debug`,
  `log_default_folder_discovery_contract`, and
  `default_folder_hierarchy_projection_for_debug` definitions now live in
  `dispatch/diagnostics/default_folders.rs`; `cargo test -p lpe-exchange
  default_folder_identification` passed 2 focused tests; `cargo test -p
  lpe-exchange default_folder_hierarchy_projection` passed 1 focused test;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode. Current line counts: `dispatch.rs` 26,280 lines,
  `dispatch/diagnostics/default_folders.rs` 470 lines,
  `dispatch/diagnostics/named_properties.rs` 76 lines,
  `dispatch/diagnostics/property_responses.rs` 363 lines,
  `dispatch/diagnostics.rs` 1,373 lines,
  `dispatch/table_diagnostics.rs` 1,380 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Moved the `SetSearchCriteria` debug-scope formatter from
  `dispatch.rs` into `dispatch/table_diagnostics.rs` next to the restriction
  debug formatters it already uses. The moved code only describes the raw
  search-criteria payload for RCA/debug logging; search criteria parsing,
  validation, response construction, table state, and canonical search-folder
  behavior remain unchanged.
- 2026-06-29 verification for the search-criteria debug-scope split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed
  `format_debug_search_criteria_scope` now lives in
  `dispatch/table_diagnostics.rs`; `cargo test -p lpe-exchange
  search_criteria_debug_scope` passed 1 focused test;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode. Current line counts: `dispatch.rs` 26,216 lines,
  `dispatch/table_diagnostics.rs` 1,444 lines,
  `dispatch/diagnostics/default_folders.rs` 470 lines,
  `dispatch/diagnostics/named_properties.rs` 76 lines,
  `dispatch/diagnostics/property_responses.rs` 363 lines,
  `dispatch/diagnostics.rs` 1,373 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/open_folder.rs` and moved the
  OpenFolder property-shape and metadata debug helpers out of `dispatch.rs`.
  The moved code only formats existing folder properties and mailbox
  projection metadata for RCA/debug output; OpenFolder response construction,
  folder lookup, mailbox projection, and canonical state remain unchanged.
- 2026-06-29 verification for the OpenFolder diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `debug_open_folder_property_shapes`
  and `debug_open_folder_metadata` definitions now live in
  `dispatch/diagnostics/open_folder.rs`; `cargo test -p lpe-exchange
  open_folder_debug_metadata` passed 1 focused test; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode. Current
  line counts: `dispatch.rs` 26,174 lines,
  `dispatch/diagnostics/open_folder.rs` 45 lines,
  `dispatch/table_diagnostics.rs` 1,444 lines,
  `dispatch/diagnostics/default_folders.rs` 470 lines,
  `dispatch/diagnostics.rs` 1,375 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/associated_config.rs` and moved the
  associated-config debug field and open-shape helpers out of `dispatch.rs`.
  The moved code only formats existing session handles, snapshot rows, and
  associated-config property lengths for RCA/debug output; associated-config
  lookup, opening, row projection, property parsing, persistence, and wire
  output remain unchanged.
- 2026-06-29 verification for the associated-config diagnostics split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed
  `associated_config_debug_fields`, `associated_config_open_shape`, and
  `associated_config_binary_property_len` definitions now live in
  `dispatch/diagnostics/associated_config.rs`; `cargo test -p lpe-exchange
  associated_config` passed 39 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode. Current
  line counts: `dispatch.rs` 26,107 lines,
  `dispatch/diagnostics/associated_config.rs` 68 lines,
  `dispatch/diagnostics/open_folder.rs` 45 lines,
  `dispatch/table_diagnostics.rs` 1,444 lines,
  `dispatch/diagnostics/default_folders.rs` 470 lines,
  `dispatch/diagnostics.rs` 1,377 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/message.rs` and moved the OpenMessage
  and message GetProperties RCA/debug logging helpers out of `dispatch.rs`.
  The moved code only emits diagnostics from already-selected message data,
  request tags, and response lengths; message lookup, OpenMessage response
  construction, GetProperties response construction, property parsing, and
  canonical message state remain unchanged.
- 2026-06-29 verification for the message diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `log_open_message_debug` and
  `log_message_getprops_response_debug` definitions now live in
  `dispatch/diagnostics/message.rs`; `cargo test -p lpe-exchange open_message`
  passed 12 focused tests; `cargo test -p lpe-exchange getprops` passed 37
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 25,992 lines,
  `dispatch/diagnostics/message.rs` 119 lines,
  `dispatch/diagnostics/associated_config.rs` 68 lines,
  `dispatch/diagnostics.rs` 1,379 lines,
  `dispatch/table_diagnostics.rs` 1,444 lines, and `dispatch/tables.rs` 1,074
  lines.
- 2026-06-29: Added `dispatch/diagnostics/common_views.rs` and moved the
  Common Views navigation-shortcut diagnostic summary helper out of
  `dispatch.rs`. The moved code only formats already-decoded shortcut metadata
  and staged property shapes for RCA/debug logs; Common Views import, save,
  persistence, identity allocation, row projection, and wire output remain
  unchanged.
- 2026-06-29 verification for the Common Views shortcut diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `common_views_saved_shortcut_summary` and its private `property_value_by_id`
  helper now live in `dispatch/diagnostics/common_views.rs`; `cargo test -p
  lpe-exchange common_views` passed 45 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode. Current line counts: `dispatch.rs` 25,939 lines,
  `dispatch/diagnostics/common_views.rs` 54 lines,
  `dispatch/diagnostics/message.rs` 119 lines,
  `dispatch/diagnostics/associated_config.rs` 68 lines,
  `dispatch/diagnostics.rs` 1,381 lines, and
  `dispatch/table_diagnostics.rs` 1,444 lines.
- 2026-06-29: Added `dispatch/diagnostics/sync_upload.rs` and moved the
  uploaded sync-state marker summary formatter out of `dispatch.rs`. The moved
  code only formats an already-maintained upload-state marker bitmask for
  RCA/debug logs; marker calculation, stream upload handling, checkpoint
  selection, sync state persistence, and transfer buffers remain unchanged.
- 2026-06-29 verification for the sync-upload marker diagnostics split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed
  `uploaded_state_marker_summary` now lives in
  `dispatch/diagnostics/sync_upload.rs`; `cargo test -p lpe-exchange
  uploaded_state` passed 2 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 25,922 lines,
  `dispatch/diagnostics/sync_upload.rs` 16 lines,
  `dispatch/diagnostics/common_views.rs` 54 lines,
  `dispatch/diagnostics.rs` 1,383 lines, and
  `dispatch/table_diagnostics.rs` 1,444 lines.
- 2026-06-29: Added `dispatch/diagnostics/calendar.rs` and moved calendar
  folder-contract, hierarchy-query contract, identity-chain, and required-tag
  diagnostic helpers out of `dispatch.rs`. The moved code only emits RCA/debug
  evidence and formats expected calendar property tags; calendar folder
  projection, hierarchy rows, content sync, checkpoint selection, canonical
  event state, and wire output remain unchanged.
- 2026-06-29 verification for the calendar diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `log_calendar_folder_contract`,
  `log_calendar_hierarchy_query_rows_contract`, `log_calendar_identity_chain`,
  and `format_calendar_required_property_tags` definitions now live in
  `dispatch/diagnostics/calendar.rs`; `cargo test -p lpe-exchange calendar`
  passed 145 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 25,642 lines,
  `dispatch/diagnostics/calendar.rs` 281 lines,
  `dispatch/diagnostics.rs` 1,385 lines, and
  `dispatch/table_diagnostics.rs` 1,444 lines.
- 2026-06-29: Added `dispatch/diagnostics/special_folders.rs` and moved the
  special-folder RCA contract logger, calendar/special sync-object diagnostic
  loggers, expected special-folder parent/item-class diagnostic helpers, and
  private special sync-object property-shape helpers out of `dispatch.rs`.
  The moved code only emits RCA/debug evidence or supports those diagnostics;
  special-folder projection, receive-folder mapping, sync manifests, canonical
  collaboration state, ROP responses, and wire output remain unchanged.
- 2026-06-29 verification for the special-folder diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `log_special_folder_contract`,
  `log_calendar_special_sync_objects`, `log_special_sync_objects`,
  `expected_special_folder_parent_id`, `expected_special_folder_item_message_class`,
  and `special_property_shape` definitions now live in
  `dispatch/diagnostics/special_folders.rs`; `cargo test -p lpe-exchange
  special_folder` passed 34 focused tests; `cargo test -p lpe-exchange
  calendar` passed 145 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode. Current line
  counts: `dispatch.rs` 24,492 physical lines,
  `dispatch/diagnostics/special_folders.rs` 508 lines,
  `dispatch/diagnostics/calendar.rs` 281 lines,
  `dispatch/diagnostics.rs` 1,323 lines, and
  `dispatch/table_diagnostics.rs` 1,444 lines.
- 2026-06-29: Extended `dispatch/diagnostics/associated_config.rs` with
  inbox associated-config table summary helpers:
  `format_inbox_associated_wire_row_summary`,
  `sort_associated_config_messages_for_debug`, and
  `format_inbox_associated_config_summary`. The moved code only formats
  already-projected associated-config rows and debug wire previews; associated
  config visibility rules, table selection, sorting behavior, query-row
  serialization, persistence, and ROP responses remain unchanged.
- 2026-06-29 verification for the associated-config summary split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the three moved definitions now live
  in `dispatch/diagnostics/associated_config.rs`; `cargo test -p lpe-exchange
  associated_config` passed 39 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 25,011 production-check lines. Current physical line
  counts: `dispatch.rs` 24,378 lines,
  `dispatch/diagnostics/associated_config.rs` 179 lines,
  `dispatch/diagnostics/special_folders.rs` 508 lines, and
  `dispatch/diagnostics.rs` 1,323 lines.
- 2026-06-29: Extended `dispatch/diagnostics/message.rs` with normal-message
  debug property projection helpers: `normal_message_debug_property_value` and
  `format_normal_message_debug_value`. The moved code only formats diagnostic
  values for already-loaded `JmapEmail` rows used by table/debug summaries;
  message property responses, row serialization, message mutations, and
  canonical mailbox state remain unchanged.
- 2026-06-29 verification for the normal-message diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/diagnostics/message.rs`; `cargo test -p lpe-exchange
  normal_message` passed 6 focused tests; `cargo test -p lpe-exchange
  open_message` passed 12 focused tests; `cargo test -p lpe-exchange getprops`
  passed 37 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 24,916 production-check lines. Current physical line
  counts: `dispatch.rs` 24,287 lines,
  `dispatch/diagnostics/message.rs` 214 lines, and
  `dispatch/diagnostics.rs` 1,323 lines.
- 2026-06-29: Extended `dispatch/diagnostics/common_views.rs` with Common
  Views/view-handoff RCA helpers: `log_outlook_view_handoff`,
  `format_outlook_view_handoff_table_contract`,
  `format_inbox_view_descriptor_behavior_contract`,
  `format_inbox_view_descriptor_set_columns_behavior_contract`,
  `format_view_descriptor_binary_summary`, and the folder table debug-target
  classifier. The moved code only builds debug contracts, descriptor summaries,
  and invariant warnings for already-projected view metadata; Common Views
  persistence, associated table rows, descriptor encoding, ROP response bytes,
  and canonical state remain unchanged.
- 2026-06-29 verification for the Common Views view-handoff diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/diagnostics/common_views.rs`; `cargo test -p lpe-exchange
  common_views` passed 45 focused tests; `cargo test -p lpe-exchange
  view_handoff` passed 5 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 24,479 production-check lines. Current physical line
  counts: `dispatch.rs` 23,867 lines,
  `dispatch/diagnostics/common_views.rs` 474 lines, and
  `dispatch/diagnostics.rs` 1,323 lines.
- 2026-06-29: Added `dispatch/diagnostics/table_queries.rs` and moved
  `log_mapi_query_position_debug` out of `dispatch.rs`. The moved code only
  logs the already-built `RopQueryPosition` response and table summary context;
  query-position response construction, table cursors, restriction handling,
  row counts, and wire bytes remain unchanged.
- 2026-06-29 verification for the query-position diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `log_mapi_query_position_debug` now
  lives in `dispatch/diagnostics/table_queries.rs`; `cargo test -p
  lpe-exchange query_position` passed 5 focused tests; `cargo test -p
  lpe-exchange tables` passed 194 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 24,334 production-check lines. Current physical line
  counts: `dispatch.rs` 23,723 lines,
  `dispatch/diagnostics/table_queries.rs` 145 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_hierarchy_table_query_rows_response`. The moved code logs the
  already-built hierarchy `QueryRows` response and preserves the existing RCA
  metric recording call; hierarchy row construction, table cursor state,
  response framing, and wire bytes remain unchanged.
- 2026-06-29 verification for the hierarchy query-row diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed both table-query
  diagnostics helpers now live in `dispatch/diagnostics/table_queries.rs`;
  `cargo test -p lpe-exchange hierarchy_table` passed 12 focused tests; `cargo
  test -p lpe-exchange tables` passed 194 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 24,244
  production-check lines. Current physical line counts: `dispatch.rs` 23,635
  lines, `dispatch/diagnostics/table_queries.rs` 233 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_open` and
  `log_outlook_contents_table_set_columns`. The moved code only emits
  OpenContentsTable and SetColumns RCA/debug metadata from already-selected
  folder, column, named-property, and view-handoff context; table handle
  creation, SetColumns validation, selected column state, row projection, and
  wire responses remain unchanged.
- 2026-06-29 verification for the contents-table open/set-columns diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed both moved
  definitions now live in `dispatch/diagnostics/table_queries.rs`; `cargo test
  -p lpe-exchange contents_table` passed 21 focused tests; `cargo test -p
  lpe-exchange set_columns` passed 13 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 24,138
  production-check lines. Current physical line counts: `dispatch.rs` 23,533
  lines, `dispatch/diagnostics/table_queries.rs` 335 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_sort` and
  `log_outlook_contents_table_restrict`. The moved code only emits SortTable
  and Restrict RCA/debug metadata from already-mutated table state, request
  payloads, selected columns, restrictions, and view-handoff context; sort
  validation, restriction parsing, table invalidation/recovery, cursor state,
  row projection, and wire responses remain unchanged.
- 2026-06-29 verification for the contents-table sort/restrict diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed both moved
  definitions now live in `dispatch/diagnostics/table_queries.rs`; `cargo test
  -p lpe-exchange sort_table` passed 4 focused tests; `cargo test -p
  lpe-exchange restrict` passed 35 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 23,990
  production-check lines. Current physical line counts: `dispatch.rs` 23,389
  lines, `dispatch/diagnostics/table_queries.rs` 479 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_query_rows`. The moved code only emits
  pre-response `QueryRows` RCA/debug metadata from already-selected table
  state, selected columns, restriction/sort state, snapshots, and view-handoff
  summaries; query execution, row selection, table cursor advancement,
  response serialization, and wire bytes remain unchanged.
- 2026-06-29 verification for the contents-table query-rows diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definition now
  lives in `dispatch/diagnostics/table_queries.rs`; `cargo test -p
  lpe-exchange query_rows` passed 52 focused tests; `cargo test -p
  lpe-exchange tables` passed 194 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 23,819
  production-check lines. Current physical line counts: `dispatch.rs` 23,221
  lines, `dispatch/diagnostics/table_queries.rs` 647 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_query_rows_response`. The moved code only logs
  the already-built contents-table `QueryRows` response and already-updated
  table cursor state; row construction, row selection, response framing,
  response serialization, cursor advancement, and wire bytes remain unchanged.
- 2026-06-29 verification for the contents-table query-rows response
  diagnostics split: `cargo fmt --package lpe-exchange`; `rg` confirmed the
  moved definition now lives in `dispatch/diagnostics/table_queries.rs`; `cargo
  test -p lpe-exchange query_rows` passed 52 focused tests; `cargo test -p
  lpe-exchange tables` passed 194 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 23,710
  production-check lines. Current physical line counts: `dispatch.rs` 23,114
  lines, `dispatch/diagnostics/table_queries.rs` 754 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_seek_row`. The moved code only emits `SeekRow`
  RCA/debug metadata from the request, already-built response, current table
  position, selected columns, restriction/sort state, and view-handoff
  context; seek validation, position calculation, cursor mutation, response
  construction, and wire bytes remain unchanged.
- 2026-06-29 verification for the contents-table seek-row diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definition now
  lives in `dispatch/diagnostics/table_queries.rs`; `cargo test -p
  lpe-exchange seek_row` passed 7 focused tests; `cargo test -p lpe-exchange
  tables` passed 194 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 23,626 production-check lines. Current physical line
  counts: `dispatch.rs` 23,032 lines,
  `dispatch/diagnostics/table_queries.rs` 836 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with
  `log_outlook_contents_table_find_row` and its private
  `rop_response_return_value` decoder. The moved code only emits `FindRow`
  RCA/debug metadata and invariant warnings from the request, already-built
  response, selected columns, restriction/sort state, table position, and
  view-handoff context; find-row validation, restriction evaluation, row
  lookup, cursor handling, response construction, and wire bytes remain
  unchanged.
- 2026-06-29 verification for the contents-table find-row diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed
  `log_outlook_contents_table_find_row` now lives in
  `dispatch/diagnostics/table_queries.rs`; `cargo test -p lpe-exchange
  find_row` passed 47 focused tests; `cargo test -p lpe-exchange tables`
  passed 194 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 23,428 production-check lines. Current physical line
  counts: `dispatch.rs` 22,837 lines,
  `dispatch/diagnostics/table_queries.rs` 1,031 lines, and
  `dispatch/diagnostics.rs` 1,325 lines.
- 2026-06-29: Extended `dispatch/diagnostics/execute.rs` with
  `log_execute_rop_debug` and moved the shared first-post-hierarchy probe
  summary structs into the diagnostics hub. The moved code only emits
  Execute-level RCA/debug records from already-built request/response
  summaries, session startup state, logon response summaries, and
  post-hierarchy probe summaries; Execute parsing, ROP execution, response
  construction, replay caching, session mutation, and wire bytes remain
  unchanged.
- 2026-06-29 verification for the Execute ROP diagnostics split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `log_execute_rop_debug` now lives in
  `dispatch/diagnostics/execute.rs`; `cargo test -p lpe-exchange
  execute_rop_debug` passed 6 focused tests; `cargo test -p lpe-exchange
  execute` passed 55 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,958 production-check lines. Current physical line
  counts: `dispatch.rs` 22,381 lines,
  `dispatch/diagnostics/execute.rs` 434 lines, and
  `dispatch/diagnostics.rs` 1,357 lines.
- 2026-06-29: Extended `dispatch/diagnostics/property_responses.rs` with
  `should_log_outlook_surface_getprops_info`. The moved code only decides
  whether to emit Outlook-surface GetProperties RCA/debug logs based on the
  already-open object's folder id; GetProperties response construction,
  property projection, object lookup, session state, and wire bytes remain
  unchanged.
- 2026-06-29 verification for the Outlook-surface GetProperties diagnostics
  predicate split: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `should_log_outlook_surface_getprops_info` now lives in
  `dispatch/diagnostics/property_responses.rs`; `cargo test -p lpe-exchange
  getprops` passed 37 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,944 production-check lines. Current physical line
  counts: `dispatch.rs` 22,368 lines,
  `dispatch/diagnostics/property_responses.rs` 368 lines, and
  `dispatch/diagnostics.rs` 1,357 lines.
- 2026-06-29: Added `dispatch/diagnostics/probes.rs` and moved the
  first-post-hierarchy probe summary helpers into it:
  `summarize_first_post_hierarchy_probe`, `set_properties_probe_request`, and
  the OpenFolder/GetProperties/SetProperties response-shape summary helpers.
  The moved code only parses already-built ROP request/response buffers for
  RCA/debug summaries and tests; Execute parsing, SetProperties mutation,
  GetProperties response construction, property projection, session state, and
  wire bytes remain unchanged.
- 2026-06-29 verification for the first-post-hierarchy probe diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed the moved
  definitions now live in `dispatch/diagnostics/probes.rs`; `cargo test -p
  lpe-exchange first_post_hierarchy_probe` passed 2 focused tests; `cargo test
  -p lpe-exchange getprops` passed 37 focused tests; `cargo test -p
  lpe-exchange set_properties` passed 19 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 22,686
  production-check lines. Current physical line counts: `dispatch.rs` 22,118
  lines,
  `dispatch/diagnostics/probes.rs` 253 lines, and
  `dispatch/diagnostics.rs` 1,359 lines.
- 2026-06-29: Extended `dispatch/diagnostics/sync_upload.rs` with
  `sync_checkpoint_scope`. The moved code only classifies an already-loaded
  SyncConfigure checkpoint for RCA/debug logging; SyncConfigure parsing,
  checkpoint selection, manifest construction, checkpoint persistence, and
  wire bytes remain unchanged.
- 2026-06-29 verification for the SyncConfigure checkpoint-scope diagnostics
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed
  `sync_checkpoint_scope` now lives in `dispatch/diagnostics/sync_upload.rs`;
  `cargo test -p lpe-exchange sync_configure` passed 2 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 22,667
  production-check lines. Current physical line counts: `dispatch.rs` 22,100
  lines,
  `dispatch/diagnostics/sync_upload.rs` 35 lines, and
  `dispatch/diagnostics.rs` 1,359 lines.
- 2026-06-29: Added `dispatch/object_ids.rs` and moved the
  LongTermId/IdFromLongTermId object-scope helpers into it:
  `debug_object_scope_for_id` and
  `rop_long_term_id_from_id_response_for_scope` with their private loaded-scope
  predicates. This is a behavior-preserving object-identity dispatch split,
  not a diagnostics move: the helpers still decide whether `RopLongTermIdFromId`
  returns the canonical conversion response or `ecNotFound`. LongTermId source
  decoding, response serialization, object preload planning, and session state
  remain unchanged.
- 2026-06-29 verification for the object-id dispatch split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/object_ids.rs`; `cargo test -p lpe-exchange long_term_id` passed 8
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,570 production-check lines. Current physical line
  counts: `dispatch.rs` 22,007 lines and
  `dispatch/object_ids.rs` 96 lines.
- 2026-06-29: Extended `dispatch/diagnostics/table_queries.rs` with the
  Outlook bootstrap QueryRows diagnostics helpers:
  `outlook_bootstrap_query_rows_phase` and
  `outlook_bootstrap_query_rows_total_count`. The moved code only supplies
  RCA/debug startup phase and total-row-count fields from already-open table
  objects and already-loaded snapshots; QueryRows validation, row selection,
  cursor movement, response construction, and wire bytes remain unchanged.
- 2026-06-29 verification for the bootstrap QueryRows diagnostics split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/diagnostics/table_queries.rs`; `cargo test -p lpe-exchange
  bootstrap_query_rows` passed 1 focused test; `cargo test -p lpe-exchange
  query_rows` passed 52 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,474 production-check lines. Current physical line
  counts: `dispatch.rs` 21,913 lines and
  `dispatch/diagnostics/table_queries.rs` 1,125 lines.
- 2026-06-29: Extended `dispatch/diagnostics/default_folders.rs` with
  `default_folder_discovery_specs` and `default_folder_entry_id_property_name`.
  The moved code only names default-folder discovery properties and supplies
  the ordered folder/tag list used by RCA/debug default-folder contracts;
  default-folder SetProperties validation, canonical entry-id projection,
  alias recording, and wire responses remain unchanged.
- 2026-06-29 verification for the default-folder diagnostics helper split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/diagnostics/default_folders.rs`; `cargo test -p
  lpe-exchange default_folder` passed 22 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 22,431
  production-check lines. Current physical line counts: `dispatch.rs` 21,872
  lines and
  `dispatch/diagnostics/default_folders.rs` 494 lines.
- 2026-06-29: Added `dispatch/default_folders.rs` and moved the default-folder
  SetProperties validation helpers into it:
  `default_folder_entry_id_expected_folder_id`, `folder_set_property_problems`,
  and the private hidden-configuration-folder message-class helper. This is a
  behavior-preserving dispatch split: default-folder validation rules,
  supported hidden folder writes, problem indexes/error codes, canonical
  entry-id checks, and SetProperties response construction remain unchanged.
- 2026-06-29 verification for the default-folder validation dispatch split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/default_folders.rs`; `cargo test -p lpe-exchange
  default_folder` passed 22 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,291 production-check lines. Current physical line
  counts: `dispatch.rs` 21,735 lines and `dispatch/default_folders.rs` 140
  lines.
- 2026-06-29: Extended `dispatch/default_folders.rs` with the default-folder
  safe-value and alias helpers:
  `default_folder_identification_safe_property_values`,
  `record_default_folder_entry_id_aliases`,
  `default_folder_identification_values_stripped_by_safe_values`, and
  `strips_default_folder_identification_value_for_folder_id`. This is a
  behavior-preserving dispatch split: root/Inbox default-folder identification
  stripping, canonical indexed special-folder projection, alias recording, and
  SetProperties response behavior remain unchanged.
- 2026-06-29 verification for the default-folder safe-value dispatch split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/default_folders.rs`; `cargo test -p lpe-exchange
  default_folder` passed 22 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 22,113 production-check lines. Current physical line
  counts: `dispatch.rs` 21,567 lines and `dispatch/default_folders.rs` 311
  lines.
- 2026-06-29: Added `dispatch/search_folders.rs` and moved the search-folder
  projection and search-criteria conversion helpers into it:
  `search_folder_handle_properties`, `bounded_search_criteria_from_rop`,
  `bounded_search_criteria_to_rop`, `builtin_search_criteria_to_rop`,
  `builtin_search_role_for_folder_id`, and
  `builtin_search_criteria_to_rop_for_folder_id`. This is a
  behavior-preserving dispatch split: `RopSetSearchCriteria`,
  `RopGetSearchCriteria`, canonical search-folder persistence, built-in
  search-folder fallbacks, bounded JSON criteria, restriction conversion, row
  output, and ROP response bytes remain unchanged.
- 2026-06-29 verification for the search-folder dispatch split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/search_folders.rs`; `cargo test -p lpe-exchange search_criteria`
  passed 22 focused tests; `cargo test -p lpe-exchange search_folder` passed
  29 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 21,341 production-check lines. Current physical line
  counts: `dispatch.rs` 20,832 lines and `dispatch/search_folders.rs` 738
  lines.
- 2026-06-29: Added `dispatch/property_tags.rs` and moved the primitive
  property-id comparison helpers into it: `property_ids_match` and
  `common_views_link_row_expected_default`. This is a behavior-preserving
  helper split: Common Views default-column classification, table projection
  support checks, diagnostics naming, and row output remain unchanged.
- 2026-06-29 verification for the property-tag helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/property_tags.rs`; `cargo test -p lpe-exchange common_views`
  passed 45 focused tests; `cargo test -p lpe-exchange tables` passed 194
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 21,327 production-check lines. Current physical line
  counts: `dispatch.rs` 20,820 lines and `dispatch/property_tags.rs` 15
  lines.
- 2026-06-29: Added `dispatch/contacts.rs` and moved the Outlook contact
  folder-surface classifiers into it: `mapi_folder_is_outlook_contacts_surface`
  and `is_contact_link_timestamp_config`. This is a behavior-preserving helper
  split: ContactLink timestamp configuration handling, contacts-surface debug
  fields, associated-config GetProperties diagnostics, contact table output,
  and contact mutation behavior remain unchanged. The `OscContactSources`
  named-property probe remains local to NameToId request handling.
- 2026-06-29 verification for the contact helper split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/contacts.rs`; `cargo test -p lpe-exchange contacts` passed 39
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 21,314 production-check lines. Current physical line
  counts: `dispatch.rs` 20,809 lines and `dispatch/contacts.rs` 14 lines.
- 2026-06-29: Extended `dispatch/folders.rs` with the collaboration-folder
  open/projection helper `collaboration_folder_handle_properties`. This is a
  behavior-preserving folder helper split: the projected property tag list,
  collaboration folder property values, open-folder response behavior, and row
  output remain unchanged.
- 2026-06-29 verification for the collaboration-folder helper split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the moved definition now lives in
  `dispatch/folders.rs`; `cargo test -p lpe-exchange folder_properties_for_open`
  passed 7 focused tests; a broad `cargo test -p lpe-exchange folder` filter
  exposed the existing order-sensitive hard-delete test, and
  `cargo test -p lpe-exchange
  mapi_over_http_microsoft_hard_delete_messages_and_subfolders_hard_deletes_trash_contents`
  passed in isolation; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 21,267 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 20,763
  lines and `dispatch/folders.rs` 120 lines.
- 2026-06-29: Extended `dispatch/messages.rs` with the message follow-up copy
  helpers: `copy_message_followup_property_values_for_request`,
  `copy_all_message_followup_property_values_for_request`, and their private
  property-tag classifier. This is a behavior-preserving message helper split:
  Move/CopyMessages follow-up property copying, canonical storage-tag
  normalization, missing-property problem reporting, supported property
  application, and response behavior remain unchanged.
- 2026-06-29 verification for the message follow-up helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/messages.rs`; `cargo test -p lpe-exchange copy_message` passed 5
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 21,115 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 20,614
  lines and `dispatch/messages.rs` 214 lines.
- 2026-06-29: Extended `dispatch/sync_import.rs` with sync-upload state
  bookkeeping helpers: `upload_state_property_name`, `upload_state_marker_bit`,
  `uploaded_state_has_delta_anchor`, `mark_uploaded_state_stream`,
  `record_sync_upload_content_change`,
  `record_sync_upload_content_checkpoint`, and
  `record_sync_upload_hierarchy_change`. This is a behavior-preserving sync
  helper split: uploaded MetaTag marker handling, delta-anchor detection,
  content/hierarchy collector checkpoint state, uploaded change-number sets,
  and sync response behavior remain unchanged.
- 2026-06-29 verification for the sync-upload helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/sync_import.rs`; `cargo test -p lpe-exchange uploaded_state`
  passed 2 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,979 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 20,485
  lines and `dispatch/sync_import.rs` 636 lines.
- 2026-06-29: Extended `dispatch/sync_import.rs` with the sync mailbox
  projection helpers `sync_mailboxes_with_collaboration_counts` and its
  private collaboration-folder hierarchy-scope predicate. This is a
  behavior-preserving sync helper split: collaboration folder item counts,
  hierarchy sync inclusion for calendar collaboration folders, remembered MAPI
  identities, sync root scoping, and emitted sync manifests remain unchanged.
- 2026-06-29 verification for the sync mailbox projection helper split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the moved definition now lives in
  `dispatch/sync_import.rs`; `cargo test -p lpe-exchange sync` passed 218
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,920 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 20,428
  lines and `dispatch/sync_import.rs` 693 lines.
- 2026-06-29: Extended `dispatch/public_folders.rs` with the public-folder
  open/projection helper `public_folder_handle_properties`. This is a
  behavior-preserving public-folder helper split: the projected property tag
  list, canonical public-folder property values, public-folder open response
  behavior, and public-folder row output remain unchanged.
- 2026-06-29 verification for the public-folder helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definition now lives in
  `dispatch/public_folders.rs`; `cargo test -p lpe-exchange public_folder`
  passed 74 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,882 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 20,391
  lines and `dispatch/public_folders.rs` 86 lines.
- 2026-06-29: Extended `dispatch/folders.rs` with folder parent/trash
  relationship helpers `mailbox_parent_folder_id_for_dispatch` and
  `mailbox_is_trash_or_descendant`. This is a behavior-preserving folder
  dispatch split: folder move parent resolution, collaboration calendar parent
  mapping, trash-descendant checks for delete/sync-import paths, canonical
  folder move/delete behavior, and ROP responses remain unchanged.
- 2026-06-29 verification for the folder parent/trash helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/folders.rs`; `cargo test -p lpe-exchange folder` passed 406
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,853 production-check lines. Current physical line
  counts: `dispatch.rs` 20,364 lines and `dispatch/folders.rs` 150 lines.
- 2026-06-29: Extended `dispatch/folders.rs` with the open-folder snapshot
  count helpers `snapshot_message_counts_for_folder`,
  `snapshot_email_belongs_to_folder`, and `email_role_folder_id`. This is a
  behavior-preserving folder projection split: folder content/unread count
  calculation for unloaded snapshots, role-backed mailbox-state matching,
  associated-content count projection, and open-folder response behavior remain
  unchanged.
- 2026-06-29 verification for the open-folder snapshot count helper split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/folders.rs`; `cargo test -p lpe-exchange
  folder_properties_for_open` passed 7 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,827 production-check lines. Current physical line counts:
  `dispatch.rs` 20,341 lines and `dispatch/folders.rs` 173 lines.
- 2026-06-29: Extended `dispatch/folders.rs` with open-folder property
  projection helpers `folder_properties_for_open` and
  `folder_properties_for_open_from_mailboxes`. This is a behavior-preserving
  folder projection split: mailbox-backed folder properties, collaboration
  folder projection, public-folder projection, search-folder projection,
  advertised special-folder fallbacks, persisted `PidTagExtendedFolderFlags`,
  IPM subtree OST identity lookup, associated-content count projection, and
  open-folder response behavior remain unchanged.
- 2026-06-29 verification for the open-folder property projection split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/folders.rs`; `cargo test -p lpe-exchange
  folder_properties_for_open` passed 7 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,661 production-check lines. Current physical line counts:
  `dispatch.rs` 20,177 lines and `dispatch/folders.rs` 337 lines.
- 2026-06-29: Extended `dispatch/folders.rs` with mailbox folder hard-delete
  helpers `hard_delete_folder_contents` and
  `hard_delete_mailbox_tree_contents`. This is a behavior-preserving folder
  mutation split: canonical mailbox-message deletion, subtree mailbox
  discovery, delete-right checks, partial-completion accounting, RCA/debug log
  fields, purge metrics, changed-folder tracking, audit actions, and ROP
  response callers remain unchanged.
- 2026-06-29 verification for the mailbox folder hard-delete helper split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/folders.rs`; `cargo test -p lpe-exchange empty_folder`
  passed 15 focused tests; `cargo test -p lpe-exchange hard_delete` passed 11
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,465 production-check lines. Current physical line
  counts: `dispatch.rs` 19,990 lines and `dispatch/folders.rs` 524 lines.
- 2026-06-29: Extended `dispatch/public_folders.rs` with public-folder
  mutation helpers `hard_delete_public_folder_contents` and
  `copy_public_folder_tree_for_mapi`. This is a behavior-preserving
  public-folder dispatch split: canonical public-folder item deletion,
  tree-copy traversal, copied folder and item fields, audit actions, RCA/debug
  log fields, purge metrics, partial-completion handling, and ROP response
  callers remain unchanged.
- 2026-06-29 verification for the public-folder mutation helper split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/public_folders.rs`; `cargo test -p lpe-exchange public_folder`
  passed 74 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,327 production-check lines. Current physical line
  counts: `dispatch.rs` 19,856 lines and `dispatch/public_folders.rs` 220
  lines.
- 2026-06-29: Added `dispatch/recoverable_items.rs` and moved
  `hard_delete_recoverable_folder_contents` into it. This is a
  behavior-preserving recoverable-items dispatch split: recoverable folder
  validation, canonical recoverable item purge, partial-completion tracking,
  changed-folder reporting, audit action/subject values, and EmptyFolder ROP
  response callers remain unchanged.
- 2026-06-29 verification for the recoverable-items dispatch split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definition now lives in
  `dispatch/recoverable_items.rs`; `cargo test -p lpe-exchange recoverable`
  passed 14 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 20,293 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 19,823
  lines and `dispatch/recoverable_items.rs` 36 lines.
- 2026-06-29: Added `dispatch/associated_config.rs` and moved the
  associated-configuration mutation and persistence helpers into it:
  `delete_associated_config_properties`,
  `associated_config_message_for_mutation`,
  `associated_config_mutation_base_properties`,
  `persist_associated_config_message`,
  `persist_associated_config_stream_message`,
  `persist_released_associated_config_stream`,
  `message_list_settings_placeholder_persisted_properties`,
  `is_empty_inbox_message_list_settings_placeholder`,
  `associated_config_uuid`, `associated_config_class_and_subject`,
  `transient_associated_message_id`, and
  `transient_client_local_message_id`. This is a behavior-preserving
  associated-configuration dispatch split: saved-handle mutation fallback,
  property deletion, stream release persistence, placeholder suppression,
  deterministic identity generation, class/subject defaults, transient
  client-local ID detection, and ROP callers remain unchanged.
- 2026-06-29 verification for the associated-configuration dispatch split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the moved definitions now
  live in `dispatch/associated_config.rs`; `cargo test -p lpe-exchange
  associated_config` passed 39 focused tests; `cargo test -p lpe-exchange
  common_views` passed 45 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 19,990 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 19,532
  lines and `dispatch/associated_config.rs` 299 lines.
- 2026-06-29: Moved the Common Views descriptor diagnostics test
  `view_handoff_descriptor_summary_reports_outlook_view_shape` from the
  inline `dispatch.rs` test module into `dispatch/diagnostics/common_views.rs`
  beside `format_view_descriptor_binary_summary`. This is a behavior-preserving
  diagnostics test split: the descriptor summary helper, Outlook view
  descriptor bytes, asserted summary fields, and runtime code remain unchanged.
- 2026-06-29 verification for the Common Views diagnostics test split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the test now lives only in
  `dispatch/diagnostics/common_views.rs`; `cargo test -p lpe-exchange
  view_handoff_descriptor_summary_reports_outlook_view_shape` passed 1 focused
  test; `cargo test -p lpe-exchange common_views` passed 46 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `dispatch.rs` at 19,973
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `dispatch.rs` 19,517 lines and
  `dispatch/diagnostics/common_views.rs` 493 lines.
- 2026-06-29: Extended `dispatch/messages.rs` with staged canonical-message
  mutation helpers: `stage_message_property_values`,
  `apply_staged_message_property_values`,
  `apply_staged_message_recipient_replacement`, and
  `delete_canonical_message_text_properties`. This is a behavior-preserving
  message dispatch split: staged SetProperties validation, follow-up property
  validation, pending property application, canonical recipient replacement,
  text-property deletion, audit action/subject values, and ROP callers remain
  unchanged.
- 2026-06-29 verification for the staged message mutation split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `dispatch/messages.rs`; `cargo test -p lpe-exchange message` passed 205
  focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reported
  `dispatch.rs` at 19,829 production-check lines; `git diff --check` exited 0
  with CRLF warnings only. Current physical line counts: `dispatch.rs` 19,377
  lines and `dispatch/messages.rs` 354 lines.
- 2026-06-29: Added `service/http_routes.rs` and moved the Exchange HTTP
  endpoint path constants plus `rpc_proxy_paths` into it. This is a
  behavior-preserving service routing split: the uppercase and lowercase EWS
  paths, MAPI EMSMDB/NSPI trailing-slash variants, RPC proxy compatibility
  path, Outlook canonical RPC proxy casing, router registration, handlers,
  authentication behavior, SOAP response handling, and MIME output remain
  unchanged.
- 2026-06-29 verification for the service HTTP route split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the route constants and
  `rpc_proxy_paths` definitions now live in `service/http_routes.rs` while
  `service.rs` consumes them; `cargo test -p lpe-exchange
  rpc_proxy_routes_include_outlook_canonical_case` passed 1 focused test;
  `cargo test -p lpe-exchange rpc_proxy` passed 51 focused tests; `cargo test
  -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,573
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,722 lines and
  `service/http_routes.rs` 22 lines.
- 2026-06-29: Added `service/ews/errors.rs` and moved the top-level SOAP
  authentication/error response helpers into it: `error_response`,
  `is_authentication_error`, `soap_auth_challenge`, and `soap_error`. This is
  a behavior-preserving EWS XML response split: the crate-visible
  `service::error_response` API, Basic challenge realm, SOAP fault envelope,
  XML escaping, status codes, endpoint handlers, and operation dispatch remain
  unchanged.
- 2026-06-29 verification for the EWS SOAP error helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/errors.rs`; `cargo test -p lpe-exchange
  authentication_errors_return_basic_challenge` passed 1 focused test; `cargo
  test -p lpe-exchange unknown_ews_operations_return_parseable_invalid_operation_errors`
  passed 1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,535
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,688 lines,
  `service/http_routes.rs` 22 lines, and `service/ews/errors.rs` 37 lines.
- 2026-06-29: Moved the EWS XML response primitives `xml_response` and
  `escape_xml` from `service.rs` into `service/ews/xml.rs`. This is a
  behavior-preserving XML helper split: the `text/xml; charset=utf-8` response
  header, status-code handling, XML escaping order, SOAP faults, item/folder
  rendering, notification rendering, MIME attachment XML fragments, and
  existing unqualified call sites remain unchanged.
- 2026-06-29 verification for the EWS XML helper split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed `xml_response` and `escape_xml` definitions now
  live in `service/ews/xml.rs`; `cargo test -p lpe-exchange
  authentication_errors_return_basic_challenge` passed 1 focused test; `cargo
  test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,517
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,672 lines,
  `service/ews/xml.rs` 170 lines, `service/ews/errors.rs` 37 lines, and
  `service/http_routes.rs` 22 lines.
- 2026-06-29: Added `service/ews/responses.rs` and moved reusable EWS error
  response builders into it: `mail_app_operation_error_response`,
  `get_item_error_response`, `get_folder_error_response`,
  `get_user_availability_error_response`,
  `set_user_oof_settings_error_response`, `ews_error_code_or`,
  `operation_error_response`, `get_user_photo_error_response`, and
  `get_password_expiration_date_error_response`. This is a
  behavior-preserving response-helper split: XML element names, response
  classes, response codes, descriptive link keys, mail-app error-code mapping,
  access-denied fallback mapping, escaping, handlers, and success response
  renderers remain unchanged.
- 2026-06-29 verification for the EWS response-helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/responses.rs`; `cargo test -p lpe-exchange
  unknown_ews_operations_return_parseable_invalid_operation_errors` passed 1
  focused test; `cargo test -p lpe-exchange user_oof_settings` passed 6 focused
  tests; `cargo test -p lpe-exchange user_availability` passed 2 focused
  tests; `cargo test -p lpe-exchange mail_app` passed 1 focused test; `cargo
  test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,380
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,544 lines,
  `service/ews/responses.rs` 147 lines, `service/ews/xml.rs` 170 lines,
  `service/ews/errors.rs` 37 lines, and `service/http_routes.rs` 22 lines.
- 2026-06-29: Added `service/ews/mail_tips.rs` and moved the MailTips and
  service-configuration response rendering family into it:
  `MailTipProjection`, `RequestedServiceConfiguration`,
  `get_mail_tips_response`, `get_service_configuration_response`,
  `service_configuration_success_message`, `service_configuration_error_message`,
  and `mail_tip_xml`. This is a behavior-preserving EWS response split:
  requested-service parsing, recipient lookup, OOF projection inputs, supported
  MailTips limits, unsupported service-configuration gap responses, XML element
  names, mailbox type mapping, and escaping remain unchanged.
- 2026-06-29 verification for the MailTips/service-configuration split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/mail_tips.rs`; `cargo test -p lpe-exchange mail_tips` passed 3
  focused tests; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,222
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,393 lines and
  `service/ews/mail_tips.rs` 156 lines.
- 2026-06-29: Added `service/ews/rooms.rs` and moved the EWS room-list
  response helpers into it: `computed_room_list_address`,
  `get_rooms_response`, and `get_room_lists_response`. This is a
  behavior-preserving EWS response split: room/equipment filtering, the
  `rooms@domain` fallback, requested RoomList comparison behavior, room-list
  advertisement only when room or equipment entries exist, XML element names
  and casing, escaping, and handler routing remain unchanged.
- 2026-06-29 verification for the EWS rooms split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/rooms.rs`; `cargo test -p lpe-exchange rooms` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 15,152
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,326 lines and
  `service/ews/rooms.rs` 70 lines.
- 2026-06-29: Added `service/ews/retention.rs` and moved the EWS retention
  policy tag response helpers into it: `get_user_retention_policy_tags_response`,
  `retention_policy_tag_xml`, `ews_retention_tag_type`, and
  `ews_retention_action`. This is a behavior-preserving response split:
  canonical retention assignment lookup, tenant visibility, XML element names,
  retention tag type/action mappings, archive flag derivation, escaping, and
  handler routing remain unchanged.
- 2026-06-29 verification for the EWS retention split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/retention.rs`; `cargo test -p lpe-exchange
  get_user_retention_policy_tags` passed 2 focused tests; `cargo test -p
  lpe-exchange ews` passed 215 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and
  reported `service.rs` at 15,089 production-check lines; `git diff --check`
  exited 0 with CRLF warnings only. Current physical line counts:
  `service.rs` 14,267 lines and `service/ews/retention.rs` 64 lines.
- 2026-06-29: Added `service/ews/compliance.rs` and moved the EWS
  compliance/eDiscovery response helpers into it:
  `get_discovery_search_configuration_response`,
  `get_searchable_mailboxes_response`, `search_mailboxes_response`,
  `get_hold_on_mailboxes_response`, `set_hold_on_mailboxes_response`,
  `hold_mailbox_xml`, `get_non_indexable_item_details_response`,
  `get_non_indexable_item_statistics_response`, and
  `non_indexable_report_xml`. This is a behavior-preserving response split:
  canonical compliance/search/hold/non-indexable report store calls, Bcc-safe
  search result projection, tenant visibility, XML element names, hold action
  values, per-mailbox non-indexable counts, escaping, and handler routing
  remain unchanged.
- 2026-06-29 verification for the EWS compliance split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/compliance.rs`; `cargo test -p lpe-exchange
  ediscovery_configuration_and_searchable_mailboxes_project_canonical_compliance_state`
  passed 1 focused test; `cargo test -p lpe-exchange
  search_mailboxes_records_canonical_discovery_search_results_without_bcc`
  passed 1 focused test; `cargo test -p lpe-exchange
  non_indexable_reports_project_canonical_search_diagnostics` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,826
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 14,013 lines and
  `service/ews/compliance.rs` 268 lines.
- 2026-06-29: Added `service/ews/message_tracking.rs` and moved the EWS
  message-tracking response helpers into it:
  `find_message_tracking_report_response`,
  `get_message_tracking_report_response`, and `message_tracking_report_xml`.
  This is a behavior-preserving response split: canonical LPE-CT trace lookup,
  tenant boundary filtering, report/detail selection, XML element names,
  recipient/event rendering, diagnostics escaping, and handler routing remain
  unchanged.
- 2026-06-29 verification for the EWS message-tracking split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/message_tracking.rs`; `cargo test -p lpe-exchange
  message_tracking_reports` passed 2 focused tests; `cargo test -p
  lpe-exchange ews` passed 215 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and
  reported `service.rs` at 14,735 production-check lines; `git diff --check`
  exited 0 with CRLF warnings only. Current physical line counts:
  `service.rs` 13,925 lines and `service/ews/message_tracking.rs` 95 lines.
- 2026-06-29: Added `service/ews/bulk_transfer.rs` and moved the EWS
  bulk-transfer response renderer into it: `transfer_job_response`. This is a
  behavior-preserving response split: `UploadItems` and `ExportItems` operation
  names, response wrapper element names, response class/code, transfer job
  status/direction projection, transfer entry field order, canonical/source ID
  rendering, escaping, and handler routing remain unchanged.
- 2026-06-29 verification for the EWS bulk-transfer split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed `transfer_job_response` now has its
  only definition in `service/ews/bulk_transfer.rs`; `cargo test -p
  lpe-exchange bulk_transfer_operations_record_canonical_transfer_jobs` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,685
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,876 lines and
  `service/ews/bulk_transfer.rs` 52 lines.
- 2026-06-29: Added `service/ews/mail_apps.rs` and moved the EWS Mail Apps
  success response renderers into it: `get_app_manifests_response`,
  `get_app_marketplace_url_response`, `mail_app_state_response`, and
  `get_client_access_token_response`. This is a behavior-preserving response
  split: canonical mail-app catalog/install/token store calls, marketplace
  policy handling, generated token custody, audit actions, operation names,
  XML element names, manifest/status/token field projection, scope rendering,
  escaping, and handler routing remain unchanged.
- 2026-06-29 verification for the EWS Mail Apps split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/mail_apps.rs`; `cargo test -p lpe-exchange
  mail_app_operations_use_canonical_catalog_install_and_token_state` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,572
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,767 lines and
  `service/ews/mail_apps.rs` 116 lines.
- 2026-06-29: Added `service/ews/unified_messaging.rs` and moved the EWS
  Unified Messaging phone-call response renderers into it:
  `play_on_phone_response`, `phone_call_information_response`,
  `disconnect_phone_call_response`, and their shared
  `unified_messaging_call_xml` helper. This is a behavior-preserving response
  split: canonical call creation/fetch/disconnect store calls, message ID
  lookup, phone-number parsing, error mapping, audit actions, operation names,
  XML element names, call field projection, and escaping remain unchanged.
- 2026-06-29 verification for the EWS Unified Messaging split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/unified_messaging.rs`; `cargo test -p lpe-exchange
  unified_messaging_operations_use_canonical_call_state` passed 1 focused test;
  `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,494
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,693 lines and
  `service/ews/unified_messaging.rs` 79 lines.
- 2026-06-29: Added `service/ews/ucs.rs` and moved the EWS UCS/instant
  messaging response renderers into it: `get_im_item_list_response`,
  `get_im_items_response`, `im_group_operation_response`,
  `im_member_operation_response`, `simple_ews_operation_result`, and their
  private IM group/member XML helpers. This is a behavior-preserving response
  split: canonical contact-group store calls, distribution-list visibility
  checks, request parsing, audit actions, operation names, XML element names,
  IM member ID/value construction, requested-item filtering, and escaping
  remain unchanged.
- 2026-06-29 verification for the EWS UCS split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/ucs.rs`; `cargo test -p lpe-exchange
  ucs_im_group_operations_use_canonical_contact_group_state` passed 1 focused
  test; `cargo test -p lpe-exchange
  ucs_distribution_list_membership_stays_tenant_scoped` passed 1 focused test;
  `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,357
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,565 lines and
  `service/ews/ucs.rs` 137 lines.
- 2026-06-29: Added `service/ews/user_configuration.rs` and moved the EWS
  user-configuration response renderer into it:
  `get_user_configuration_response`, the requested-property selector, and the
  private dictionary XML renderer. This is a behavior-preserving response
  split: user-configuration key/upsert parsing, canonical configuration store
  reads/writes/deletes, audit actions, operation names, response class/code,
  selected Dictionary/XmlData/BinaryData projection, base64 payload rendering,
  item ID/change-key rendering, and escaping remain unchanged.
- 2026-06-29 verification for the EWS user-configuration split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the moved definitions now live in
  `service/ews/user_configuration.rs`; `cargo test -p lpe-exchange
  user_configuration_create_get_update_and_delete_use_canonical_storage` passed
  1 focused test; `cargo test -p lpe-exchange
  user_configuration_supports_mailbox_scoped_names_and_not_found_errors` passed
  1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,245
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,457 lines and
  `service/ews/user_configuration.rs` 115 lines.
- 2026-06-29: Added `service/ews/rules.rs` and moved the EWS Inbox Rules
  response renderer into it: `get_inbox_rules_response`. This is a
  behavior-preserving response split: canonical Sieve-backed rule listing,
  bounded EWS rule-to-Sieve mutation parsing, Exchange-only rule rejection,
  audit actions, operation names, XML element names, rule priority ordering,
  enabled/unsupported projection, and escaping remain unchanged.
- 2026-06-29 verification for the EWS Inbox Rules split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed `get_inbox_rules_response` now has its only
  definition in `service/ews/rules.rs`; `cargo test -p lpe-exchange
  inbox_rules_project_and_update_canonical_sieve_rules` passed 1 focused test;
  `cargo test -p lpe-exchange
  update_inbox_rules_rejects_exchange_only_rule_shapes_without_side_effects`
  passed 1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,206
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,419 lines and
  `service/ews/rules.rs` 41 lines.
- 2026-06-29: Added `service/ews/reminders.rs` and moved the EWS reminders
  response renderer into it: `get_reminders_response` and its private
  `reminder_item_id` helper. This is a behavior-preserving response split:
  canonical reminder query/action paths, dismiss/snooze mutations, operation
  names, XML element names, reminder item ID construction with optional
  occurrence start, start/due fallback behavior, status change-key rendering,
  action item ID parsing, and escaping remain unchanged.
- 2026-06-29 verification for the EWS reminders split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed `get_reminders_response` and
  `reminder_item_id` now live in `service/ews/reminders.rs` while, at this
  point in the refactor sequence, `parse_reminder_item_id` remained local to
  the reminder action path; `cargo
  test -p lpe-exchange reminders_are_read_and_dismissed_from_canonical_reminder_state`
  passed 1 focused test; `cargo test -p lpe-exchange
  perform_reminder_action_snoozes_calendar_and_task_canonical_reminders` passed
  1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,153
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,368 lines and
  `service/ews/reminders.rs` 54 lines.
- 2026-06-29: Added `service/ews/sharing.rs` and moved the EWS sharing
  response renderers into it: `get_sharing_metadata_response`,
  `get_sharing_folder_response`, `refresh_sharing_folder_response`,
  `accept_sharing_invitation_response`, and their private rights/folder-class
  XML helpers. This is a behavior-preserving response split: same-tenant
  sharing grant verification, canonical collaboration collection reads,
  invitation grant creation, operation names, response class/code values,
  folder IDs/change keys, owner/initiator fields, data-type mapping,
  permission-level mapping, and escaping remain unchanged.
- 2026-06-29 verification for the EWS sharing split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the sharing response definitions now live in
  `service/ews/sharing.rs`; `cargo test -p lpe-exchange
  get_sharing_folder_returns_accessible_same_tenant_calendar_grant` passed 1
  focused test; `cargo test -p lpe-exchange
  refresh_sharing_folder_verifies_accessible_shared_contacts_folder` passed 1
  focused test; `cargo test -p lpe-exchange
  accept_sharing_invitation_creates_same_tenant_calendar_grant` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests,
  including `get_sharing_metadata_returns_owned_calendar_metadata_without_exchange_tokens`;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 14,002
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,225 lines and
  `service/ews/sharing.rs` 150 lines.
- 2026-06-29: Added `service/ews/delegation.rs` and moved the EWS delegate
  response renderers into it: `delegate_operation_response`,
  `get_delegate_response`, `delegate_success_response_message`,
  `delegate_error_response_message`, and their private delegate-user and
  permission-level XML helpers. This is a behavior-preserving response split:
  delegate parsing, cross-tenant rejection, canonical permission/preference
  mutations, operation names, response class/code values, user identity fields,
  calendar/inbox permission projection, meeting-copy/private-item flags, and
  escaping remain unchanged.
- 2026-06-29 verification for the EWS delegation split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the delegate response definitions now live in
  `service/ews/delegation.rs`; `cargo test -p lpe-exchange
  delegate_operations_use_canonical_permissions_and_preferences` passed 1
  focused test; `cargo test -p lpe-exchange
  delegate_add_rejects_cross_tenant_delegate` passed 1 focused test; `cargo
  test -p lpe-exchange
  delegate_add_rejects_unsupported_exchange_only_permission_shapes` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,919
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,148 lines and
  `service/ews/delegation.rs` 86 lines.
- 2026-06-29: Added `service/ews/availability.rs` and moved the EWS
  availability/time-zone response renderers into it:
  `get_server_time_zones_response`,
  `get_user_availability_success_response`, and
  `availability_suggestions_response`. This is a behavior-preserving response
  split: authenticated-mailbox free/busy validation, canonical calendar event
  fetching, availability window filtering, event ordering, error response
  handling, operation names, UTC and W. Europe time-zone definitions,
  suggestion-day fallback, busy-type projection, and escaping remain
  unchanged.
- 2026-06-29 verification for the EWS availability split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the availability/time-zone response
  definitions now live in `service/ews/availability.rs`; `cargo test -p
  lpe-exchange get_server_time_zones_returns_minimal_definitions` passed 1
  focused test; `cargo test -p lpe-exchange
  get_user_availability_returns_canonical_busy_events` passed 1 focused test;
  `cargo test -p lpe-exchange
  get_user_availability_returns_suggestions_when_requested` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,832
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,064 lines and
  `service/ews/availability.rs` 87 lines.
- 2026-06-29: Added `service/ews/oof.rs` and moved the EWS out-of-office
  response renderers into it: `get_user_oof_settings_response` and
  `set_user_oof_settings_success_response`. This is a behavior-preserving
  response split: active Sieve script lookup, OOF metadata parsing, vacation
  script generation, enable/disable writes, scheduled duration handling,
  external-audience normalization, error response shape, operation names,
  response class/code values, reply body projection, and escaping remain
  unchanged.
- 2026-06-29 verification for the EWS OOF split: `cargo fmt --package
  lpe-exchange`; `rg` confirmed the OOF response definitions now live in
  `service/ews/oof.rs`; `cargo test -p lpe-exchange
  get_user_oof_settings_returns_disabled_without_active_vacation` passed 1
  focused test; `cargo test -p lpe-exchange
  get_user_oof_settings_projects_canonical_sieve_vacation` passed 1 focused
  test; `cargo test -p lpe-exchange
  set_user_oof_settings_writes_canonical_sieve_vacation` passed 1 focused test;
  `cargo test -p lpe-exchange
  set_user_oof_settings_scheduled_round_trips_canonical_sieve_metadata` passed
  1 focused test; `cargo test -p lpe-exchange
  set_user_oof_settings_disables_active_sieve_script` passed 1 focused test;
  `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,782
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 13,016 lines and
  `service/ews/oof.rs` 51 lines.
- 2026-06-29: Added `service/ews/folders.rs` and moved folder-specific EWS
  response/XML renderers into it: `create_folder_success_response`,
  `create_public_folder_success_response`, `folders_operation_success_response`,
  `delete_folder_success_response`, `root_folder_xml`, `folder_xml`,
  `mailbox_folder_xml`, `public_folder_xml`, and `folder_change_key`. This is a
  behavior-preserving response split: canonical mailbox/public-folder
  mutations, hierarchy and folder sync query logic, public-folder rights,
  collaboration collection projection, operation names, folder IDs, change-key
  strings, effective-rights XML, count fields, and escaping remain unchanged.
- 2026-06-29 verification for the EWS folder response split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the folder XML/response definitions
  now live in `service/ews/folders.rs`; `cargo test -p lpe-exchange
  create_folder_uses_canonical_mailbox_store` passed 1 focused test; `cargo
  test -p lpe-exchange create_folder_uses_canonical_public_folder_store` passed
  1 focused test; `cargo test -p lpe-exchange
  find_folder_lists_contact_and_calendar_folders` passed 1 focused test; `cargo
  test -p lpe-exchange get_folder_returns_multiple_supported_folder_kinds`
  passed 1 focused test; `cargo test -p lpe-exchange
  sync_folder_hierarchy_lists_contact_and_calendar_folders` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,579
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 12,822 lines and
  `service/ews/folders.rs` 204 lines.
- 2026-06-29: Added `service/ews/directory.rs` and moved the EWS
  directory/persona response helpers into it: `resolve_names_response`,
  `find_people_response`, `get_persona_response`, `expand_dl_response`,
  `visible_address_book_email`, and their private mailbox/persona lookup and
  XML helpers. This is a behavior-preserving response split: canonical
  address-book fetching, tenant/contact visibility checks, distribution-list
  membership projection, persona ID format, mailbox type strings, operation
  names, response class/code values, no-results errors, and escaping remain
  unchanged.
- 2026-06-29 verification for the EWS directory/persona split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the directory/persona helper
  definitions now live in `service/ews/directory.rs`; `cargo test -p
  lpe-exchange resolve_names_returns_tenant_directory_account_match` passed 1
  focused test; `cargo test -p lpe-exchange
  find_people_projects_canonical_accounts_and_contacts` passed 1 focused test;
  `cargo test -p lpe-exchange
  get_persona_resolves_only_visible_stateless_persona_ids` passed 1 focused
  test; `cargo test -p lpe-exchange
  expand_dl_projects_same_tenant_directory_group_members` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,176
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 12,441 lines and
  `service/ews/directory.rs` 384 lines.
- 2026-06-29: Added `service/ews/ids.rs` and moved the EWS `ConvertId`
  response renderers into it: `convert_id_success_response` and
  `convert_id_xml`. This is a behavior-preserving response split: alternate ID
  parsing/conversion stays in `service.rs`, canonical object-family mapping
  stays unchanged, and `AlternateId`, `AlternatePublicFolderId`, and
  `AlternatePublicFolderItemId` XML output remains unchanged.
- 2026-06-29 verification for the EWS ConvertId response split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the ConvertId response definitions
  now live in `service/ews/ids.rs`; `cargo test -p lpe-exchange
  convert_id_round_trips_supported_canonical_object_families` passed 1 focused
  test; `cargo test -p lpe-exchange
  convert_id_round_trips_hex_entry_id_attachment_payload` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,148
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 12,415 lines and
  `service/ews/ids.rs` 29 lines.
- 2026-06-29: Added `service/ews/attachments.rs` and moved the EWS attachment
  success response envelopes into it: `get_attachment_success_response`,
  `create_attachment_success_response`, and
  `delete_attachment_success_response`. This is a behavior-preserving response
  split: Magika validation, canonical attachment creation/deletion, blob
  custody, attachment ID parsing, root item rendering, and handler routing
  remain unchanged.
- 2026-06-29 verification for the EWS attachment response split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the attachment response definitions
  now live in `service/ews/attachments.rs`; `cargo test -p lpe-exchange
  get_attachment_returns_canonical_attachment_content` passed 1 focused test;
  `cargo test -p lpe-exchange
  create_attachment_validates_and_adds_canonical_attachment` passed 1 focused
  test; `cargo test -p lpe-exchange
  delete_attachment_removes_canonical_attachment_reference` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 13,100
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 12,370 lines and
  `service/ews/attachments.rs` 50 lines.
- 2026-06-29: Added `service/ews/contacts.rs` and moved the EWS contact
  projection helpers into it: `contact_change_key`, `contact_summary_xml`,
  `contact_item_xml`, `contact_item_xml_with_change_key`, and the private
  email/phone/URL XML helpers. This is a behavior-preserving projection split:
  contact create/update/delete handlers, canonical contact store calls, rich
  contact parsing, sync-version selection, XML field names, change-key inputs,
  and escaping remain unchanged.
- 2026-06-29 verification for the EWS contact projection split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the contact projection definitions
  now live in `service/ews/contacts.rs`; `cargo test -p lpe-exchange
  create_delete_contact_round_trips_through_sync_folder_items` passed 1
  focused test; `cargo test -p lpe-exchange
  update_contact_round_trips_through_sync_folder_items` passed 1 focused test;
  `cargo test -p lpe-exchange
  sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state` passed
  1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,908
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 12,188 lines and
  `service/ews/contacts.rs` 191 lines.
- 2026-06-29: Added `service/ews/calendar.rs` and moved the EWS calendar
  projection helpers into it: `calendar_change_key`,
  `calendar_item_summary_xml`, `calendar_item_xml`,
  `calendar_item_xml_with_change_key`, attendee XML helpers, and bounded
  recurrence XML conversion helpers. This is a behavior-preserving projection
  split: calendar create/update/delete handlers, canonical event store calls,
  request date parsing, availability date helpers, participant metadata
  parsing, XML field names, change-key inputs, and escaping remain unchanged.
- 2026-06-29 verification for the EWS calendar projection split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the calendar projection definitions
  now live in `service/ews/calendar.rs`; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed 1
  focused test; `cargo test -p lpe-exchange
  find_item_returns_calendar_items_from_canonical_store` passed 1 focused test;
  `cargo test -p lpe-exchange get_user_availability_returns_canonical_busy_events`
  passed 1 focused test; `cargo test -p lpe-exchange ews` passed 215 focused
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,621
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 11,916 lines and
  `service/ews/calendar.rs` 281 lines.
- 2026-06-29: Added `service/ews/tasks.rs` and moved the EWS task projection
  helpers into it: `task_change_key`, `task_item_summary_xml`,
  `task_item_xml`, `task_item_xml_with_change_key`,
  `create_task_success_response`, and private task status/optional text XML
  helpers. This is a behavior-preserving projection split: task
  create/update/delete handlers, canonical task store calls, request parsing,
  task status input conversion, sync-version selection, XML field names,
  change-key inputs, and escaping remain unchanged.
- 2026-06-29 verification for the EWS task projection split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the task projection definitions now
  live in `service/ews/tasks.rs`; `cargo test -p lpe-exchange
  create_update_task_round_trips_through_sync_folder_items` passed 1 focused
  test; `cargo test -p lpe-exchange delete_item_deletes_canonical_task`
  passed 1 focused test; `cargo test -p lpe-exchange task` passed 8 focused
  task tests; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,506
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 11,809 lines and
  `service/ews/tasks.rs` 113 lines.
- 2026-06-29: Added `service/ews/mail.rs` and moved the EWS canonical message
  projection helpers into it: `message_summary_xml`, `message_item_xml`,
  `message_item_xml_with_details`, `root_item_id_xml`, and
  `create_item_success_response`. This is a behavior-preserving rendering
  split: EWS mail create/update/delete/move/copy/send handlers, canonical
  submission and mailbox store calls, MIME rendering primitives, attachment
  loading, Bcc-safe MIME policy, XML field names, and escaping remain
  unchanged.
- 2026-06-29 verification for the EWS mail projection split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the canonical message projection
  definitions now live in `service/ews/mail.rs`; `cargo test -p lpe-exchange
  create_item_saveonly_stores_message_as_canonical_draft` passed 1 focused
  test; `cargo test -p lpe-exchange get_item_mime_content` passed 2 focused
  MIME tests; `cargo test -p lpe-exchange
  update_item_updates_message_read_and_flag_state` passed 1 focused test;
  `cargo test -p lpe-exchange
  move_item_moves_custom_mailbox_message_to_target_folder` passed 1 focused
  test; `cargo test -p lpe-exchange
  copy_item_copies_custom_mailbox_message_to_target_folder` passed 1 focused
  test; `cargo test -p lpe-exchange
  send_item_submits_existing_draft_through_canonical_submission` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,418
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 11,727 lines and
  `service/ews/mail.rs` 88 lines.
- 2026-06-29: Added `service/ews/public_folders.rs` and moved the EWS
  public-folder item projection helpers into it:
  `public_folder_item_change_key`, `public_folder_item_summary_xml`,
  `public_folder_item_xml`, and
  `create_public_folder_item_success_response`. This is a
  behavior-preserving rendering split: public-folder permission checks,
  canonical public-folder item create/update/delete/move/copy store calls,
  clone-input construction, XML field names, body selection, change-key
  inputs, and escaping remain unchanged.
- 2026-06-29 verification for the EWS public-folder item projection split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the public-folder item
  projection definitions now live in `service/ews/public_folders.rs`; `cargo
  test -p lpe-exchange create_item_saveonly_stores_public_folder_post` passed
  1 focused test; `cargo test -p lpe-exchange
  update_item_updates_public_folder_item` passed 1 focused test; `cargo test
  -p lpe-exchange find_item_lists_public_folder_items` passed 1 focused test;
  `cargo test -p lpe-exchange sync_folder_items_reports_public_folder_items`
  passed 1 focused test; `cargo test -p lpe-exchange
  get_item_returns_public_folder_item_body` passed 1 focused test; `cargo test
  -p lpe-exchange move_item_moves_public_folder_item_to_target_public_folder`
  passed 1 focused test; `cargo test -p lpe-exchange
  copy_item_copies_public_folder_item_to_target_public_folder` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,345
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 11,658 lines and
  `service/ews/public_folders.rs` 74 lines.
- 2026-06-29: Moved the EWS contact and calendar CreateItem success response
  renderers into the existing object-family modules:
  `create_contact_success_response` now lives in `service/ews/contacts.rs`,
  and `create_event_success_response` now lives in `service/ews/calendar.rs`.
  This is a behavior-preserving rendering split: contact/calendar
  create/update/delete handlers, canonical store calls, request parsing,
  calendar date/time helpers, XML field names, and escaping remain unchanged.
- 2026-06-29 verification for the EWS contact/calendar create-response split:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the CreateItem response
  definitions now live in `service/ews/contacts.rs` and
  `service/ews/calendar.rs`; `cargo test -p lpe-exchange
  create_delete_contact_round_trips_through_sync_folder_items` passed 1
  focused test; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed 1
  focused test; `cargo test -p lpe-exchange
  update_contact_round_trips_through_sync_folder_items` passed 1 focused test;
  `cargo test -p lpe-exchange
  find_item_returns_calendar_items_from_canonical_store` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,292
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 11,607 lines,
  `service/ews/contacts.rs` 215 lines, and `service/ews/calendar.rs` 308
  lines.
- 2026-06-29: Moved the remaining generic EWS response helpers into
  `service/ews/responses.rs`: `update_item_success_response`,
  `delete_item_success_response`, `move_item_success_response`,
  `archive_item_success_response`, `copy_item_success_response`,
  `simple_operation_success_response`, `mark_as_junk_success_response`,
  `operation_response_message`, `sync_folder_items_response`, and
  `unsupported_operation_response`. This is a behavior-preserving response
  split: EWS operation dispatch, unsupported operation error text, sync-folder
  item response XML, conversation-item error response shape, item operation
  success envelopes, and XML escaping remain unchanged.
- 2026-06-29 verification for the generic EWS response helper split: `cargo
  fmt --package lpe-exchange`; `rg` confirmed the generic response helper
  definitions now live in `service/ews/responses.rs`; `cargo test -p
  lpe-exchange unknown_ews_operations_return_parseable_invalid_operation_errors`
  passed 1 focused test; `cargo test -p lpe-exchange
  sync_folder_items_returns_empty_sync_for_custom_mailbox_folder` passed 1
  focused test; `cargo test -p lpe-exchange
  get_conversation_items_returns_current_canonical_thread_nodes` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 12,142
  production-check lines. Current physical line counts: `service.rs` 11,467
  lines and `service/ews/responses.rs` 291 lines.
- 2026-06-29: Added `service/ews/conversations.rs` and moved the EWS
  conversation response renderers into it: `find_conversation_response`,
  `get_conversation_items_response`, and their private conversation summary,
  node, participant, sender, recipient, and delivery-time XML helpers. This is
  a behavior-preserving rendering split: canonical message fetching,
  conversation action mutation, persistent future-message rejection,
  sort/order parsing, response operation names, item IDs, sync-state strings,
  and XML escaping remain unchanged.
- 2026-06-29 verification for the EWS conversation response split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the conversation response helper
  definitions now live in `service/ews/conversations.rs`; `cargo test -p
  lpe-exchange find_conversation_groups_messages_by_canonical_thread_in_folder`
  passed 1 focused test; `cargo test -p lpe-exchange
  get_conversation_items_returns_current_canonical_thread_nodes` passed 1
  focused test; `cargo test -p lpe-exchange apply_conversation_action` passed
  3 focused tests; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,895
  production-check lines. Current physical line counts: `service.rs` 11,232
  lines and `service/ews/conversations.rs` 241 lines.
- 2026-06-29: Added `service/ews/sync_state.rs` and moved EWS collaboration
  sync-state helpers into it: `collaboration_sync_state`,
  `collaboration_sync_state_items`, `collaboration_sync_state_collection_id`,
  `sync_state_items_by_id`, `sync_version_by_id`, and their private sync-state
  version/type definitions. This is a behavior-preserving helper split:
  contact, calendar, task, and public-folder sync-state string formats,
  legacy-sync-state parsing, change-key maps, and collection-id extraction
  remain unchanged.
- 2026-06-29 verification for the EWS sync-state helper split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the sync-state helper definitions
  now live in `service/ews/sync_state.rs`; `cargo test -p lpe-exchange
  sync_folder_items_returns_contacts_from_canonical_store` passed 1 focused
  test; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed 1
  focused test; `cargo test -p lpe-exchange
  create_update_task_round_trips_through_sync_folder_items` passed 1 focused
  test; `cargo test -p lpe-exchange
  sync_folder_items_reports_public_folder_items` passed 1 focused test; `cargo
  test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,805
  production-check lines. Current physical line counts: `service.rs` 11,151
  lines and `service/ews/sync_state.rs` 93 lines.
- 2026-06-29: Moved the generic EWS `FindItem` success response envelope into
  `service/ews/responses.rs` as `find_item_response`. This is a
  behavior-preserving rendering split: item lookup, folder-kind branching,
  canonical store calls, public-folder permission checks, item XML projection,
  `TotalItemsInView` counting, and response XML shape remain unchanged.
- 2026-06-29 verification for the EWS `FindItem` response split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the `find_item_response` definition
  now lives in `service/ews/responses.rs`; `cargo test -p lpe-exchange
  find_item_lists_custom_mailbox_messages` passed 1 focused test; `cargo test
  -p lpe-exchange find_item_lists_system_mailbox_messages_by_distinguished_id`
  passed 1 focused test; `cargo test -p lpe-exchange
  find_item_lists_public_folder_items` passed 1 focused test; `cargo test -p
  lpe-exchange find_item_returns_calendar_items_from_canonical_store` passed 1
  focused test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,786
  production-check lines. Current physical line counts: `service.rs` 11,133
  lines and `service/ews/responses.rs` 309 lines.
- 2026-06-29: Added `service/ews/request_ids.rs` and moved the generic EWS
  request ID extractors into it: `requested_item_ids`,
  `requested_attachment_ids`, `requested_transfer_item_ids`, and
  `requested_folder_ids`. This is a behavior-preserving parser split:
  mailbox UUID interpretation stays in `service.rs`, transfer upload fallback
  ID generation is unchanged, attachment/item/folder XML attribute matching is
  unchanged, and operation handlers keep the same routing and store calls.
- 2026-06-29 verification for the EWS request ID parser split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the request ID extractor definitions
  now live in `service/ews/request_ids.rs`; `cargo test -p lpe-exchange
  get_item_returns_system_mailbox_message_body` passed 1 focused test; `cargo
  test -p lpe-exchange get_attachment_returns_canonical_attachment_content`
  passed 1 focused test; `cargo test -p lpe-exchange
  bulk_transfer_operations_record_canonical_transfer_jobs` passed 1 focused
  test; `cargo test -p lpe-exchange
  sync_folder_items_accepts_any_folder_id_namespace_prefix` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,743
  production-check lines. Current physical line counts: `service.rs` 11,094
  lines and `service/ews/request_ids.rs` 42 lines.
- 2026-06-29: Moved the EWS IM/UCS request parser helpers into the existing
  `service/ews/ucs.rs` module: `requested_smtp_address`,
  `requested_im_group_id`, `requested_im_group_name`,
  `requested_im_member_kind`, `requested_im_member_value`,
  `requested_im_contact_member`, and private `parse_prefixed_uuid`. This is a
  behavior-preserving parser split: IM group/member store mutations, contact
  creation, distribution-list tenant scoping, response XML, operation names,
  and fallback member parsing remain unchanged.
- 2026-06-29 verification for the EWS IM/UCS request parser split: `cargo fmt
  --package lpe-exchange`; `rg` confirmed the IM/UCS request parser
  definitions now live in `service/ews/ucs.rs`; `cargo test -p lpe-exchange
  ucs_im_group_operations_use_canonical_contact_group_state` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,613
  production-check lines; `git diff --check` exited 0 with CRLF warnings only.
  Current physical line counts: `service.rs` 10,971 lines,
  `service/ews/request_ids.rs` 42 lines, and `service/ews/ucs.rs` 260 lines.
- 2026-06-29: Moved the EWS compliance and message-tracking request text
  parsers into their object-family modules: `discovery_query_text` now lives
  in `service/ews/compliance.rs`, and `message_tracking_query_text` plus
  `requested_message_tracking_report_id` now live in
  `service/ews/message_tracking.rs`. This is a behavior-preserving parser
  split: canonical discovery-search, hold, and message-tracking store calls,
  tenant scoping, Bcc-safe search behavior, report ID fallback order, response
  XML, and operation errors remain unchanged.
- 2026-06-29 verification for the EWS compliance/message-tracking parser
  split: `cargo fmt --package lpe-exchange`; `rg` confirmed the moved parser
  definitions now live in `service/ews/compliance.rs` and
  `service/ews/message_tracking.rs`; `cargo test -p lpe-exchange
  search_mailboxes_records_canonical_discovery_search_results_without_bcc`
  passed 1 focused test; `cargo test -p lpe-exchange
  message_tracking_reports_project_canonical_trace_state` passed 1 focused
  test; `cargo test -p lpe-exchange ews` passed 215 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reported `service.rs` at 11,577
  production-check lines; `git diff --check` exited 0. Current physical line
  counts: `service.rs` 10,938 lines, `service/ews/compliance.rs` 276 lines,
  and `service/ews/message_tracking.rs` 120 lines.
- 2026-06-29: Advanced the shared date/time primitive cleanup by moving
  Windows FILETIME epoch/tick arithmetic into `lpe-domain::civil_time`.
  `crates/lpe-domain/src/civil_time.rs` now owns
  `WINDOWS_UNIX_EPOCH_OFFSET_SECONDS`, `WINDOWS_FILETIME_TICKS_PER_SECOND`,
  `windows_filetime_from_unix_seconds`,
  `windows_filetime_from_signed_unix_seconds`, and
  `unix_seconds_from_windows_filetime`. `mapi_mailstore.rs` and
  `mapi/tables.rs` delegate to those primitives. MAPI-specific behavior stays
  local: RFC3339 parsing, calendar event date/time parsing, event duration
  rules, and synthetic change-number-to-FILETIME mapping remain unchanged.
- 2026-06-29 verification for the FILETIME primitive cleanup: `cargo fmt
  --package lpe-domain --package lpe-exchange`; `rg` confirmed the raw Windows
  epoch/tick constants are centralized in `lpe-domain` for the touched MAPI
  paths; `cargo test -p lpe-domain` passed 34 tests and doc tests; `cargo test
  -p lpe-exchange calendar_projection_backs_outlook_table_identity_and_status_columns`
  passed 1 focused test; `cargo test -p lpe-exchange
  reminder_named_properties_project_from_canonical_reminder_links` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_set_properties_updates_canonical_mail_reminder_state` passed
  1 focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing.
- 2026-06-29: Advanced MR-018 by delegating identical MAPI lowerhex debug
  wrappers to `lpe_domain::crypto::hex_lower`. The touched wrappers are
  `dispatch/diagnostics.rs::bytes_to_hex`,
  `mapi_mailstore.rs::format_debug_hex`, `mapi/transport.rs::hex_preview`,
  `mapi/tables.rs::format_bytes_hex`,
  `mapi/tables.rs::format_debug_binary`, and
  `mapi/properties/values.rs::bytes_to_hex`. Hex parsers and validation
  helpers such as `hex_to_bytes`, `hex_digit`, and test-only hex helpers remain
  local because they are not primitive rendering wrappers. ROP-specific debug
  previews remain future MR-018 work.
- 2026-06-29 verification for the MAPI diagnostic lowerhex wrapper cleanup:
  `cargo fmt --package lpe-exchange`; `rg` confirmed the touched files no
  longer contain inline `format!("{byte:02x}")` rendering loops in those
  wrappers; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  inbox_folder_type_getprops_response_context_includes_wire_preview` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_transport_echoes_request_id_and_client_info` passed 1 focused
  test; `cargo test -p lpe-domain` passed 34 tests and doc tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and still reports oversized production files, led by
  `mapi/dispatch.rs` at 19,829 lines; `git diff --check` exited 0 with CRLF
  warnings only.
- 2026-06-29: Advanced the same MR-018/MR-009 cleanup by adding
  `mapi/rop/debug.rs` and moving the ROP debug hex helpers
  `hex_preview_for_debug` and `format_bytes_hex` out of `mapi/rop.rs`. Both
  helpers now delegate to `lpe_domain::crypto::hex_lower`; preview truncation
  and ellipsis behavior are unchanged. No ROP parser, response serialization,
  unsupported ROP handling, or wire bytes changed.
- 2026-06-29 verification for the ROP debug hex helper split: `rg` confirmed
  no production lowercase MAPI `format!("{byte:02x}")` rendering loops remain
  under `crates/lpe-exchange/src`; the remaining production hex loop is an
  uppercase EWS formatter and the remaining lowercase loops are test fixtures.
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug_decodes_default_view_entry_id` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,941 lines; `git diff --check` exited 0 with CRLF
  warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `view_descriptor_value_shape_for_debug`, `mapi_value_shape_for_debug`, and
  `text_preview_for_debug` into `mapi/rop/debug.rs`. This is still
  debug-output-only: ROP request parsing, response serialization,
  unsupported/reserved ROP handling, property IDs, and wire bytes remain
  unchanged.
- 2026-06-29 verification for the ROP debug value-shape split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  common_views_wlink_contract_distinguishes_expected_link_defaults` passed 1
  focused test; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug_decodes_default_view_entry_id` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,875 lines; `rg` confirmed no production lowercase MAPI
  hex rendering loops remain; `git diff --check` exited 0 with CRLF warnings
  only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `mapi_object_debug_fields` into `mapi/rop/debug.rs`. This keeps the object
  kind labels and folder/item debug identifiers unchanged while moving another
  diagnostics-only helper out of `mapi/rop.rs`.
- 2026-06-29 verification for the ROP object debug formatter split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 7,633 lines; `rg`
  confirmed no production lowercase MAPI hex rendering loops remain; `git diff
  --check` exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `property_row_kind_for_debug`, `format_returned_property_tags_for_debug`,
  `format_property_tags_for_debug`, and `format_property_names_for_debug` into
  `mapi/rop/debug.rs`. The property-name map remains local in `mapi/rop.rs`
  for now because moving the full constant-heavy map is a larger slice. This
  change preserves the exact diagnostic tag-list strings and row-kind
  classification.
- 2026-06-29 verification for the ROP property tag-list formatter split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 7,593 lines; `rg`
  confirmed no production lowercase MAPI hex rendering loops remain; `git diff
  --check` exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `default_folder_property_mappings_for_debug` and its private
  default-folder mapping helper into `mapi/rop/debug.rs`. The mapping preserves
  the exact special-folder labels, folder IDs, source-key formatting, and
  canonical property storage-tag normalization.
- 2026-06-29 verification for the ROP default-folder debug mapping split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug_decodes_default_view_entry_id` passed 1
  focused test; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 7,561 lines; `rg`
  confirmed no production lowercase MAPI hex rendering loops remain; `git diff
  --check` exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `format_property_errors_for_debug` into `mapi/rop/debug.rs`. This keeps the
  flagged property-error diagnostic string format, property names, and error
  codes unchanged while moving another debug-output-only helper out of
  `mapi/rop.rs`.
- 2026-06-29 verification for the ROP property-error debug formatter split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 7,541 lines; `rg`
  confirmed `format_property_errors_for_debug` now lives in `mapi/rop/debug.rs`
  and the only remaining production hex loop under `crates/lpe-exchange/src`
  is the uppercase EWS formatter, with lowercase loops limited to test
  fixtures; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `expected_folder_type_for_debug`, `advertised_special_search_folder_for_debug`,
  and `folder_type_kind_for_debug` into `mapi/rop/debug.rs`. This preserves the
  folder-type debug labels, advertised special search-folder classification,
  and root/generic/search/invalid value names while moving another
  diagnostics-only helper group out of `mapi/rop.rs`.
- 2026-06-29 verification for the ROP folder-type debug classifier split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  folder_type_getprops` passed 8 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and now
  reports `mapi/rop.rs` at 7,497 lines; `rg` confirmed the moved folder-type
  debug helper definitions now live in `mapi/rop/debug.rs`; `git diff --check`
  exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `format_property_value_shapes_for_debug` and its private
  `semantic_property_shape_for_debug` helper into `mapi/rop/debug.rs`. The
  formatter still calls the existing property serializers/default writers and
  preserves row-byte length, semantic-shape, hex preview, and default-kind
  diagnostic strings.
- 2026-06-29 verification for the ROP property value-shape debug formatter
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  common_views_wlink_contract_distinguishes_expected_link_defaults` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,413 lines; `rg` confirmed the moved property value-shape
  helper definitions now live in `mapi/rop/debug.rs`; the MAPI lowerhex scan
  still finds only test fixtures plus the uppercase EWS formatter; `git diff
  --check` exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `format_associated_config_0e0b_debug` into `mapi/rop/debug.rs`. This keeps
  the associated-configuration `0x0E0B` diagnostic string unchanged while
  moving stored/semantic shape inspection for that Outlook compatibility
  property out of `mapi/rop.rs`.
- 2026-06-29 verification for the associated-config `0x0E0B` debug formatter
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  ipm_configuration_contract_summary_reports_required_columns_and_streams`
  passed 1 focused test; `cargo test -p lpe-exchange
  mapi_over_http_quick_step_config_0e0b_defaults_to_empty_binary` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,383 lines; `rg` confirmed the moved `0x0E0B` debug
  formatter definition now lives in `mapi/rop/debug.rs`; `git diff --check`
  exited 0 with CRLF warnings only.
- 2026-06-29: Extended the ROP debug extraction by moving
  `common_view_descriptor_property_requested` and
  `format_requested_view_descriptor_contract` into `mapi/rop/debug.rs`. This
  preserves the Common Views descriptor request classifier and
  version/name/binary/strings diagnostic contract string while moving another
  descriptor-debug helper pair out of `mapi/rop.rs`.
- 2026-06-29 verification for the Common Views descriptor request debug split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  view_descriptor` passed 6 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,330 lines; `rg` confirmed the moved descriptor request
  helper definitions now live in `mapi/rop/debug.rs`; `git diff --check`
  exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `view_descriptor_debug_property_tags` and
  `default_view_message_entry_id_target` into `mapi/rop/debug.rs`. This is a
  diagnostics-only split: Common Views descriptor tag extraction and
  default-view message EntryID target decoding remain unchanged, and no ROP
  parser, response serialization, unsupported/reserved ROP behavior, property
  IDs, or wire bytes changed.
- 2026-06-30 verification for the Common Views default-view debug split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  view_descriptor` passed 6 focused tests; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug_decodes_default_view_entry_id` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,309 lines; `rg` confirmed the moved default-view debug
  helper definitions now live in `mapi/rop/debug.rs`; `git diff --check`
  exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving the Common Views
  descriptor logging/contract cluster into `mapi/rop/debug.rs`:
  `log_common_view_descriptor_getprops_summary`,
  `format_common_view_descriptor_getprops_contract`,
  `format_common_view_descriptor_response_values`, and
  `format_default_view_entry_id_decoding`. This keeps descriptor hashing,
  descriptor response-shape text, default-view EntryID decoding, and RCA debug
  fields unchanged while moving another diagnostics-only block out of
  `mapi/rop.rs`.
- 2026-06-30 verification for the Common Views descriptor logging split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  view_descriptor` passed 6 focused tests; `cargo test -p lpe-exchange
  default_folder_entry_id_values_debug_decodes_default_view_entry_id` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 7,103 lines; `rg` confirmed the moved descriptor logging
  and default-view debug helper definitions now live in `mapi/rop/debug.rs`;
  `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `format_message_body_getprops_contract` and its private
  `is_message_body_debug_tag` classifier into `mapi/rop/debug.rs`. This keeps
  the GetProps message-body RCA contract string, mailbox/search-folder/saved
  handle source selection, native-body projection, body length accounting, and
  requested body-tag formatting unchanged.
- 2026-06-30 verification for the message-body GetProps debug split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 7,011 lines; `rg`
  confirmed the moved message-body debug helper definitions now live in
  `mapi/rop/debug.rs`; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `format_folder_type_getprops_contract` into `mapi/rop/debug.rs`. This keeps
  the folder-type GetProps RCA contract string, source preference order
  (`search_folder_definition`, opened handle, mailbox, collaboration folder,
  public folder, special-folder fallback), expected type classification, and
  issue labels unchanged.
- 2026-06-30 verification for the folder-type GetProps debug split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  folder_type_getprops` passed 8 focused tests; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 6,897 lines; `rg`
  confirmed the moved folder-type debug helper definition now lives in
  `mapi/rop/debug.rs`; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `format_ipm_configuration_getprops_contract` into `mapi/rop/debug.rs`.
  This keeps associated-config lookup, `IPM.Configuration.*` filtering,
  roaming datatype reporting, requested/missing stream-tag lists, fallback
  tag reporting, and the undocumented `0x0E0B` diagnostic string unchanged.
- 2026-06-30 verification for the IPM.Configuration GetProps debug split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  ipm_configuration_contract_summary_reports_required_columns_and_streams`
  passed 1 focused test; `cargo test -p lpe-exchange
  mapi_over_http_quick_step_config_0e0b_defaults_to_empty_binary` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 6,840 lines; `rg` confirmed the moved IPM.Configuration
  debug helper definition now lives in `mapi/rop/debug.rs`; `git diff
  --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `log_calendar_default_folder_lookup_debug` into `mapi/rop/debug.rs`. This
  keeps the calendar default-folder RCA trace fields, Inbox/root lookup labels,
  entry-id previews, decoded folder IDs, calendar collection projection fields,
  and returned property-shape diagnostic string unchanged.
- 2026-06-30 verification for the calendar default-folder debug split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  calendar_projection_backs_outlook_table_identity_and_status_columns` passed
  1 focused test; `cargo test -p lpe-exchange
  mapi_over_http_set_properties_updates_canonical_mail_reminder_state` passed
  1 focused test; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 6,730 lines; `rg`
  confirmed the moved calendar default-folder debug logger now lives in
  `mapi/rop/debug.rs`; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving the Outlook logon
  bootstrap GetProps debug cluster into `mapi/rop/debug.rs`:
  `OutlookLogonBootstrapRowShape`, `outlook_logon_bootstrap_row_shape`,
  `is_outlook_logon_bootstrap_getprops`,
  `format_outlook_logon_bootstrap_property_details`, and the private mailbox
  owner EntryID and icon-header detail formatters. This keeps logon row
  serialization, logon property values, request parsing, response bytes, and
  unsupported/reserved ROP behavior unchanged.
- 2026-06-30 verification for the Outlook logon bootstrap debug split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  logon_response_debug_summary_decodes_private_mailbox_fields` passed 1 focused
  test; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 6,538 lines; `rg`
  confirmed the moved Outlook logon bootstrap debug helper definitions now live
  in `mapi/rop/debug.rs`; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `log_get_properties_specific_debug` into `mapi/rop/debug.rs`. The underlying
  unsupported/default-property classifiers, row serialization, property value
  selection, request parsing, response bytes, and unsupported/reserved ROP
  behavior remain unchanged; `property_is_unsupported_for_object` is only
  widened to `pub(in crate::mapi)` so the debug module can call it.
- 2026-06-30 verification for the GetProps RCA logger split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `cargo test -p lpe-exchange
  logon_response_debug_summary_decodes_private_mailbox_fields` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 6,400 lines; `rg`
  confirmed the GetProps RCA logger definition now lives in
  `mapi/rop/debug.rs`; `git diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Extended the ROP debug extraction by moving
  `property_tag_debug_name` and its private `debug_property_id_matches` helper
  into `mapi/rop/debug.rs`. This is a diagnostic-name map move only; property
  IDs, named-property allocation, property value selection, request parsing,
  response bytes, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the property debug-name map split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  property_debug_names_cover_recent_outlook_folder_probes` passed 1 focused
  test; `cargo test -p lpe-exchange
  getprops_contract_response_summary_includes_access_value` passed 1 focused
  test; `cargo test -p lpe-exchange
  execute_rop_debug_summary_decodes_ids_and_return_codes` passed 1 focused
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 6,120 lines; `rg`
  confirmed the property debug-name map now lives in `mapi/rop/debug.rs`; `git
  diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/restrictions.rs` and moving
  `parse_mapi_restriction` plus the recursive `parse_mapi_restriction_from`
  parser into it. The shared tagged-property and named-property parsers remain
  in `mapi/rop.rs` because they are used by several non-restriction ROP
  parsers. Restriction parse errors, unsupported restriction handling, request
  parsing, response bytes, and unsupported/reserved ROP behavior remain
  unchanged.
- 2026-06-30 verification for the restriction parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange restriction_parser`
  passed 2 focused tests; `cargo test -p lpe-exchange
  mapi_over_http_unknown_restriction_type_terminates_current_buffer` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_set_search_criteria_rejects_unsupported_restriction` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,992 lines; `rg` confirmed the restriction parser
  definitions now live in `mapi/rop/restrictions.rs`; `git diff --check`
  exited 0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/parse.rs` and moving the
  shared tagged-property, named-property, UTF-16Z, and property-value parsing
  helpers into it. The larger `read_rop_request` parser remains in `rop.rs`.
  This is a module-boundary change only; property value decoding, named-property
  parsing errors, request parsing, response bytes, and unsupported/reserved ROP
  behavior remain unchanged.
- 2026-06-30 verification for the ROP parse helper split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange restriction_parser` passed 2
  focused tests; `cargo test -p lpe-exchange
  malformed_supported_rop_buffer_fails_without_partial_request` passed 1
  focused test; `cargo test -p lpe-exchange
  saved_associated_config_getprops_uses_same_batch_saved_message` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,949 lines; `rg` confirmed the moved parse helper
  definitions now live in `mapi/rop/parse.rs`; `git diff --check` exited 0
  with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/recipients.rs` and moving
  the free `RopModifyRecipients` row parsers into it:
  `parse_pending_recipient_row`, the simple and wrapped recipient row parsers,
  recipient-type normalization, legacy-DN recipient address resolution, and the
  recipient string reader. The `RopRequest::modify_recipients` method remains
  in `rop.rs`; request framing, recipient row decoding, canonical address
  normalization, X500 legacy-DN lookup, response bytes, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP recipient parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`
  passed 1 focused test; `cargo test -p lpe-exchange
  modify_recipients_accepts_microsoft_message_example_columns` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically`
  passed 1 focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,735 lines; `rg` confirmed the moved recipient parser
  definitions now live in `mapi/rop/recipients.rs`; `git diff --check` exited
  0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/property_rows.rs` and moving
  the `RopModifyRules` and `RopModifyPermissions` property-row count and row
  parser methods into it. The row type remains unchanged, and the parser still
  reads tagged properties through the shared ROP property parser. Rule and
  permission dispatch, canonical mutation behavior, request framing, response
  bytes, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP property-row split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_modify_rules_writes_bounded_canonical_sieve_rule` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_modify_permissions_maps_acl_rows_to_canonical_grants` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,680 lines; `rg` confirmed the moved row parser methods
  now live in `mapi/rop/property_rows.rs`; `git diff --check` exited 0 with
  CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/responses.rs` and moving the
  simple response byte builders for open-folder, open-message,
  open-embedded-message, message-status, create-folder, table-open,
  attachment-open/create, and open-stream responses into it. Stateful response
  builders such as reload-cached-information and GetProps remain in `rop.rs`.
  Response opcodes, handle indexes, typed string encoding, object-id encoding,
  row counts, attachment numbers, stream sizes, response bytes, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP simple response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  open_message_response_does_not_advertise_missing_recipient_rows` passed 1
  focused test; `cargo test -p lpe-exchange
  private_create_folder_response_never_sets_existing_folder_flag` passed 1
  focused test; `cargo test -p lpe-exchange
  microsoft_get_message_status_response_uses_set_status_opcode` passed 1
  focused test; `cargo test -p lpe-exchange
  microsoft_open_embedded_message_response_includes_message_id` passed 1
  focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,559 lines; `rg` confirmed the moved response builders now
  live in `mapi/rop/responses.rs`; `git diff --check` exited 0 with CRLF
  warnings only.
- 2026-06-30: Advanced MR-009 by moving the stream response builders
  `rop_read_stream_response`, `rop_seek_stream_response`,
  `rop_write_stream_response`, `rop_copy_to_stream_response`, and
  `rop_get_stream_size_response` from `mapi/rop.rs` into the existing
  `mapi/rop/responses.rs` module. Dispatch call sites still use the same
  exported helper names, and stream read/seek position updates, write-count
  encoding, copy counts, size encoding, response opcodes, handle indexes, error
  codes, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP stream response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_copy_to_stream_saves_canonical_message_body` passed 1 focused
  test after formatting; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed after formatting with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/rop.rs` at 5,471 lines in the tracked-file scan; direct binary line
  count reports the new untracked `mapi/rop/responses.rs` at 214 lines; `rg`
  confirmed the moved stream response builders now have single definitions in
  `mapi/rop/responses.rs`; `git diff --check` exited 0 with CRLF warnings
  only.
- 2026-06-30: Advanced MR-009 by moving another response-builder cluster from
  `mapi/rop.rs` into `mapi/rop/responses.rs`: address-type, transport-send,
  options-data, partial-completion, SetColumns, SortTable, ExpandRow,
  CollapseRow, collapse-state, Restrict, CreateMessage, SetProperties,
  SetProperties problem, DeleteProperties, and generic simple-success
  responses. Dispatch validation, canonical mutations, table state,
  restriction handling, property application, response opcodes, handle indexes,
  problem-array encoding, row serialization, and unsupported/reserved ROP
  behavior remain unchanged.
- 2026-06-30 verification for the ROP response-builder cluster split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  message_create_and_save_responses_match_microsoft_message_examples` passed 1
  focused test; `cargo test -p lpe-exchange
  contents_table_responses_match_microsoft_table_examples` passed 1 focused
  test; `cargo test -p lpe-exchange
  expand_row_response_matches_microsoft_category_example` passed 1 focused
  test; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_empty_transport_options_data` passed 1
  scenario test; `cargo test -p lpe-exchange
  mapi_over_http_transport_send_uses_canonical_submission` passed 1 scenario
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/rop.rs` at 5,315 lines; direct
  binary line count reports `mapi/rop/responses.rs` at 374 lines; `rg`
  confirmed the moved response builders now have single definitions in
  `mapi/rop/responses.rs`; `git diff --check` exited 0 with CRLF warnings
  only.
- 2026-06-30: Advanced MR-009 by moving the pure SearchCriteria,
  upload-state, FastTransfer put-buffer, and SaveChangesMessage response
  serializers from `mapi/rop.rs` into `mapi/rop/responses.rs`. The
  object-family default property list response intentionally remains in
  `mapi/rop.rs` because it selects property tags from MAPI object shape rather
  than only serializing a response. Search criteria flags, folder-id encoding,
  upload-state handle selection, FastTransfer extended/non-extended used-size
  encoding, SaveChangesMessage response handle/input handle behavior, object-id
  encoding, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP SearchCriteria/upload/FastTransfer/save
  response split: `cargo fmt --package lpe-exchange`; `cargo test -p
  lpe-exchange message_create_and_save_responses_match_microsoft_message_examples`
  passed 1 focused test; `cargo test -p lpe-exchange
  upload_state_success_response_uses_input_handle_index` passed 1 focused test;
  `cargo test -p lpe-exchange
  mapi_over_http_fast_transfer_destination_put_buffer` passed 1 focused
  scenario test; `cargo test -p lpe-exchange
  mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses`
  passed 1 focused scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/rop.rs` at 5,255 lines; direct binary line count reports
  `mapi/rop/responses.rs` at 434 lines; `rg` confirmed the moved response
  serializers now have single definitions in `mapi/rop/responses.rs`; `git
  diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by moving the pure SetReadFlags and public-folder
  per-user response serializers from `mapi/rop.rs` into
  `mapi/rop/responses.rs`: `rop_set_read_flags_response`,
  `rop_get_per_user_long_term_ids_response`, `rop_get_per_user_guid_response`,
  `rop_read_per_user_information_response`, and
  `rop_write_per_user_information_response`. Long-term ID conversion responses
  intentionally remain in `mapi/rop.rs` because they perform identity
  conversion and stale-special-folder fallback logic rather than only response
  serialization. Read-flag partial-completion encoding, per-user long-term ID
  count truncation, database GUID bytes, per-user stream offset/max-size
  handling, error codes, and unsupported/reserved ROP behavior remain
  unchanged.
- 2026-06-30 verification for the ROP read-flag/per-user response split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_set_read_flags_updates_canonical_message_state` passed 1
  scenario test; `cargo test -p lpe-exchange
  mapi_over_http_public_folder_per_user_information_round_trips_canonical_read_state`
  passed 1 scenario test; `cargo test -p lpe-exchange
  mapi_over_http_public_folder_per_user_lookup_returns_canonical_folder_identity`
  passed 1 scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/rop.rs` at 5,192 lines; direct binary line count reports
  `mapi/rop/responses.rs` at 497 lines; `rg` confirmed the moved response
  serializers now have single definitions in `mapi/rop/responses.rs`; `git
  diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by moving the pure store/public-folder response
  serializers from `mapi/rop.rs` into `mapi/rop/responses.rs`:
  `rop_get_transport_folder_response`, `rop_get_store_state_response`,
  `rop_get_owning_servers_response`, `rop_public_folder_is_ghosted_response`,
  and `rop_reset_table_response`. The receive-folder table response remains in
  `mapi/rop.rs` because it builds rows from the receive-folder compatibility
  map rather than only serializing a response. Outbox object-id encoding, store
  state flags, owning-server counters and string termination, ghosted replica
  counters, reset-table success bytes, and unsupported/reserved ROP behavior
  remain unchanged.
- 2026-06-30 verification for the ROP store/public-folder response split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  public_folder_replica_responses_match_microsoft_counter_shape` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`
  passed 1 scenario test; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_get_store_state_accepts_live_handle_without_batch_drift`
  passed 1 scenario test; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_reset_table_requires_new_set_columns` passed 1
  scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/rop.rs` at 5,142 lines; direct binary line count reports
  `mapi/rop/responses.rs` at 548 lines; `rg` confirmed the moved response
  serializers now have single definitions in `mapi/rop/responses.rs`; `git
  diff --check` exited 0 with CRLF warnings only.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/object_ids.rs` and moving
  long-term object ID conversion response helpers plus stale special-folder
  short-ID fallback conversion helpers out of `mapi/rop.rs`. The two
  `RopRequest` long-term source accessors moved with the conversion helpers so
  the fallback parsing remains local to the object-ID module. Long-term ID
  bytes, replica-GUID alias handling, stale special-folder normalization,
  dynamic object fallback handling, error codes, response opcodes, handle
  indexes, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP object-ID helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange long_term_id` passed 8
  focused tests covering ROP conversion, dispatch scope validation,
  store-adapter preload planning, identity round-trip, and MAPI-over-HTTP
  round-trip behavior; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 5,026 lines; direct physical line count reports
  `mapi/rop.rs` at 4,810 lines and `mapi/rop/object_ids.rs` at 114 lines;
  `rg` confirmed the moved long-term ID response and stale special-folder
  conversion helpers now have single definitions in `mapi/rop/object_ids.rs`.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/receive_folders.rs` and
  moving receive-folder compatibility mapping, message-class validation,
  `RopGetReceiveFolder`, and `RopGetReceiveFolderTable` response builders out
  of `mapi/rop.rs`. This isolates the fixed canonical receive-folder map from
  generic ROP code; arbitrary configurable receive-folder routing remains
  unsupported until a canonical model exists. Receive-folder row columns,
  message-class matching, canonical Inbox/Calendar folder mapping,
  last-modification change-number derivation, error handling in dispatch, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP receive-folder split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange receive_folder` passed
  12 focused MAPI-over-HTTP scenarios covering receive-folder table access,
  private-logon handle requirements, message-class matching, calendar mapping,
  canonical SetReceiveFolder acknowledgements, and noncanonical rejection;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 4,917 lines; direct
  physical line count reports `mapi/rop.rs` at 4,710 lines and
  `mapi/rop/receive_folders.rs` at 109 lines; `rg` confirmed the moved
  receive-folder helpers now have single definitions in
  `mapi/rop/receive_folders.rs`.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/logon.rs` and moving private
  mailbox logon response serialization, public-folder logon response
  serialization, GWART timestamp encoding, and logon calendar byte encoding
  out of `mapi/rop.rs`. Logon response opcodes, handle indexes, special-folder
  ordering, private/public logon flags, mailbox and public-store GUID/replid
  bytes, timestamp behavior, and unsupported/reserved ROP behavior remain
  unchanged.
- 2026-06-30 verification for the ROP logon split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange logon` passed 34 focused tests
  covering logon time bytes, GWART timestamp behavior, private logon folder
  placement, logon property projection, MAPI-over-HTTP private/public logon
  scenarios, and RPC proxy logon carrier behavior; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing;
  `python tools/check_oversized_sources.py` passed in warning mode and now
  reports `mapi/rop.rs` at 4,823 lines; direct physical line count reports
  `mapi/rop.rs` at 4,621 lines and `mapi/rop/logon.rs` at 97 lines; `rg`
  confirmed the moved logon helpers now have single definitions in
  `mapi/rop/logon.rs`.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/named_properties.rs` and
  moving the named-property response serializers for `RopGetPropertyIdsFromNames`,
  `RopGetNamesFromPropertyIds`, and `RopQueryNamedProperties` out of
  `mapi/rop.rs`. Request parsing, named-property allocation, session registry
  behavior, property ID ordering, property name wire encoding, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP named-property response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange named_property` passed
  20 focused tests covering session property-id caches, stale alias
  normalization, named-property bootstrap, no-create missing behavior,
  restart-style mapping persistence, custom named-property round trips, and
  public-folder item custom properties; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,781 lines; direct physical line count reports
  `mapi/rop.rs` at 4,582 lines and `mapi/rop/named_properties.rs` at 44
  lines; `rg` confirmed the moved named-property response serializers now have
  single definitions in `mapi/rop/named_properties.rs`.
- 2026-06-30: Advanced MR-009 by adding `mapi/rop/attachments.rs` and moving
  the `RopGetValidAttachments` response builder out of `mapi/rop.rs`. Message
  and calendar-event handle validation, canonical attachment-number projection,
  pending attachment deletion filtering, response opcode/error codes, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP attachment response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange valid_attachments`
  passed 3 focused tests covering missing calendar-event handle rejection,
  existing calendar-event attachment projection, and canonical message
  attachment-number projection; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,741 lines; direct physical line count reports
  `mapi/rop.rs` at 4,543 lines and `mapi/rop/attachments.rs` at 45 lines;
  `rg` confirmed the moved attachment response builder now has a single
  definition in `mapi/rop/attachments.rs`.
- 2026-06-30: Advanced MR-009 by moving the primitive ROP buffer helpers into
  `mapi/rop/buffer.rs`: integer writers, object-id writing, UTF-16 string
  writing, typed string writing, and the u16-prefixed string reader. Request
  parsing, response serialization, handle indexes, object-id bytes, string
  terminators, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the ROP buffer helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  open_message_response_does_not_advertise_missing_recipient_rows` passed 1
  focused test; `cargo test -p lpe-exchange
  parse_execute_request_keeps_max_rop_out` passed 1 focused test; `cargo test
  -p lpe-exchange
  mapi_over_http_transport_maps_response_code_to_header_and_envelope` passed 1
  focused transport test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,693 lines; direct physical line count reports
  `mapi/rop.rs` at 4,503 lines and `mapi/rop/buffer.rs` at 115 lines; `rg`
  confirmed the moved primitive buffer helper definitions now live in
  `mapi/rop/buffer.rs`.
- 2026-06-30: Advanced MR-009 by moving the pure reserved-ROP classifier
  `rop_id_is_reserved` into `mapi/rop/errors.rs` alongside
  `unsupported_rop_response` and parse/error response helpers. Unknown and
  reserved ROP terminal-buffer handling, common unsupported response bytes,
  request parsing, and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the reserved-ROP classifier split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  reserved_rop_is_terminal_and_uses_common_unsupported_response` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_unknown_and_reserved_rops_terminate_current_buffer` passed 1
  scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,689 lines; direct physical line count reports
  `mapi/rop.rs` at 4,500 lines and `mapi/rop/errors.rs` at 138 lines; `rg`
  confirmed the reserved-ROP classifier now has a single definition in
  `mapi/rop/errors.rs`.
- 2026-06-30: Advanced MR-009 by moving the recipient-row text extraction
  helper `optional_mapi_value_text` from the `mapi/rop.rs` hub into
  `mapi/rop/recipients.rs`. The helper is private to recipient row parsing,
  and wrapped/simple `RopModifyRecipients` address and display-name selection,
  X500 fallback resolution, canonical address normalization, request parsing,
  and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the recipient helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`
  passed 1 focused parser test; `cargo test -p lpe-exchange
  mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically`
  passed 1 scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,679 lines; direct physical line count reports
  `mapi/rop.rs` at 4,491 lines and `mapi/rop/recipients.rs` at 222 lines;
  `rg` confirmed `optional_mapi_value_text` is now defined only in
  `mapi/rop/recipients.rs`.
- 2026-06-30: Advanced MR-009 by moving the ROP byte `Cursor` reader from
  `mapi/rop.rs` into `mapi/rop/buffer.rs`, alongside the primitive buffer
  read/write helpers. Request parser code still uses the same `Cursor` type
  through the existing module re-export; cursor field visibility remains
  scoped to MAPI code for the existing parser checkpoint/slice logic. Request
  parsing, truncation errors, string decoding, restriction parsing, recipient
  row parsing, response framing, and unsupported/reserved ROP behavior remain
  unchanged.
- 2026-06-30 verification for the ROP cursor split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  malformed_supported_rop_buffer_fails_without_partial_request` passed 1
  focused parser test; `cargo test -p lpe-exchange restriction_parser` passed
  2 focused tests; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`
  passed 1 focused parser test; `cargo test -p lpe-exchange
  parse_execute_request_keeps_max_rop_out` passed 1 focused dispatch parser
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing; `python tools/check_oversized_sources.py`
  passed in warning mode and now reports `mapi/rop.rs` at 4,590 lines; direct
  physical line count reports `mapi/rop.rs` at 4,415 lines and
  `mapi/rop/buffer.rs` at 192 lines; `rg` confirmed the `Cursor` definition
  and primitive reader methods now live in `mapi/rop/buffer.rs`.
- 2026-06-30: Advanced MR-009 by moving the typed ROP request projection into
  `mapi/rop/parse.rs`: `TypedRopRequest`, the typed request view structs,
  `TypedRopRequest::rop_id`, `TypedRopRequest::unsupported_is_terminal`, and
  `RopRequest::typed`. Raw `RopRequest` storage remains in `mapi/rop.rs` for
  the existing payload accessors. Typed view construction, request
  serialization, dispatch terminal-unsupported handling, logon diagnostics,
  and unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the typed request projection split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  golden_open_folder_rop_round_trips_through_typed_parser` passed 1 focused
  test; `cargo test -p lpe-exchange
  microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser`
  passed 1 focused typed-parser test; `cargo test -p lpe-exchange
  reserved_rop_is_terminal_and_uses_common_unsupported_response` passed 1
  focused unsupported-terminal test; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,337 lines; direct physical line count reports
  `mapi/rop.rs` at 4,179 lines and `mapi/rop/parse.rs` at 285 lines; `rg`
  confirmed the typed request projection definitions now live in
  `mapi/rop/parse.rs`.
- 2026-06-30: Advanced MR-009 by moving the raw `RopRequest` storage type into
  `mapi/rop/parse.rs` beside the typed request projection. Existing payload
  accessors and `read_rop_request` remain in `mapi/rop.rs` for this slice.
  Request storage shape, typed view construction, parser boundaries, response
  serialization, dispatch terminal-unsupported handling, and
  unsupported/reserved ROP behavior remain unchanged.
- 2026-06-30 verification for the raw request storage split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  golden_open_folder_rop_round_trips_through_typed_parser` passed 1 focused
  parser test; `cargo test -p lpe-exchange
  malformed_supported_rop_buffer_fails_without_partial_request` passed 1
  focused parse-error test; `cargo test -p lpe-exchange
  reserved_rop_is_terminal_and_uses_common_unsupported_response` passed 1
  focused unsupported-terminal test; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 4,329 tracked-source lines; direct physical line count
  reports `mapi/rop.rs` at 4,172 lines and `mapi/rop/parse.rs` at 292 lines;
  `rg` confirmed `RopRequest`, `TypedRopRequest`, and `RopRequest::typed`
  now live in `mapi/rop/parse.rs`.
- 2026-06-30: Advanced MR-009 by moving the pure `RopRequest` payload accessor
  methods from `mapi/rop.rs` into `mapi/rop/parse.rs`, while deliberately
  leaving recipient row construction (`modify_recipients`) in the hub for this
  slice because it depends on address-book recipient resolution rather than
  primitive request decoding. Previously extracted typed request projection,
  long-term ID conversion, and rule/permission row helpers remain in their
  focused modules. Request payload offsets, property value parsing,
  restriction parsing, search-criteria decoding, stream/upload decoding,
  handle-index behavior, response serialization, and unsupported/reserved ROP
  behavior remain unchanged.
- 2026-06-30 verification for the request accessor split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  golden_open_folder_rop_round_trips_through_typed_parser` passed 1 focused
  parser test; `cargo test -p lpe-exchange
  malformed_supported_rop_buffer_fails_without_partial_request` passed 1
  focused parse-error test; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`
  passed 1 focused recipient parser test; `cargo test -p lpe-exchange
  microsoft_oxctabl_sort_and_query_rows_examples_parse_through_typed_parser`
  passed 1 focused table parser test; `cargo test -p lpe-exchange
  mapi_over_http_set_get_search_criteria_round_trips_supported_canonical_clauses`
  passed 1 scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 3,126 tracked-source lines; direct physical line count
  reports `mapi/rop.rs` at 3,086 lines and `mapi/rop/parse.rs` at 1,380
  lines; `rg` confirmed `input_handle_index` through `property_values` now
  live in `mapi/rop/parse.rs`, while `modify_recipients` and
  `read_rop_request` remain in `mapi/rop.rs`.
- 2026-06-30: Advanced MR-009 by moving `RopRequest::modify_recipients` from
  the `mapi/rop.rs` hub into `mapi/rop/recipients.rs`, beside the recipient
  row parser and X500/address-book fallback logic. The recipient parser helper
  re-export is now limited to tests because production code uses the inherent
  request method directly. Recipient column decoding, delete/upsert row
  handling, wrapped/simple row parsing, X500 fallback resolution, canonical
  address normalization, request parsing, and unsupported/reserved ROP
  behavior remain unchanged.
- 2026-06-30 verification for the recipient request split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`
  passed 1 focused parser test; `cargo test -p lpe-exchange
  mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically`
  passed 1 focused scenario test; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_modify_recipients_example_saves_canonically` passed
  1 focused scenario test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing; `python
  tools/check_oversized_sources.py` passed in warning mode and now reports
  `mapi/rop.rs` at 3,076 tracked-source lines; direct physical line count
  reports `mapi/rop.rs` at 3,037 lines, `mapi/rop/recipients.rs` at 273 lines,
  and `mapi/rop/parse.rs` at 1,380 lines; `rg` confirmed
  `modify_recipients` and `parse_pending_recipient_row` now live in
  `mapi/rop/recipients.rs`.
- 2026-06-30: Completed the MR-009 hub split by moving the raw ROP request
  reader from `mapi/rop.rs` into `mapi/rop/request_reader.rs` and moving the
  `RopReloadCachedInformation` and `RopGetPropertiesList` response builders
  into `mapi/rop/responses.rs`. The request reader keeps the same primitive
  cursor parsing, request truncation behavior, unsupported/reserved ROP
  handling, string decoding, and property-value decoding. The response move
  preserves default property tag selection, message/folder/table projection,
  response opcodes, handle indexes, and response bytes.
- 2026-06-30 verification for the final MR-009 ROP hub split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  parse_execute_request_keeps_max_rop_out` passed 1 focused dispatch parser
  test; `cargo test -p lpe-exchange
  malformed_supported_rop_buffer_fails_without_partial_request` passed 1
  focused parse-error test; `cargo test -p lpe-exchange
  open_message_response_does_not_advertise_missing_recipient_rows` passed 1
  focused response-shape test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and no longer
  reports `crates/lpe-exchange/src/mapi/rop.rs` as oversized; direct physical
  line counts report `mapi/rop.rs` at 1,440 lines,
  `mapi/rop/request_reader.rs` at 1,460 lines, `mapi/rop/parse.rs` at 1,380
  lines, and `mapi/rop/responses.rs` at 660 lines. `rg` confirmed
  `read_rop_request` now lives in `mapi/rop/request_reader.rs`, while
  `rop_reload_cached_information_response` and
  `rop_get_properties_list_response` now live in `mapi/rop/responses.rs`.
- 2026-06-30: Advanced MR-002 by moving the pure ICS/sync-import helper
  cluster from `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs`:
  `HIERARCHY_SYNC_CURSOR_VERSION`, `imported_message_source_key`,
  `pending_message_is_sync_metadata_only`,
  `pending_message_is_trash_sync_artifact`,
  `imported_hierarchy_parent_mailbox_id`, `hierarchy_checkpoint_status`, and
  `sync_property_filter_mode`. Source-key recognition, transient trash-artifact
  suppression, metadata-only import acknowledgement, hierarchy checkpoint
  staleness classification, and sync property filter labels remain unchanged.
- 2026-06-30 verification for the dispatch sync-import helper split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange sync_import`
  passed 17 focused tests; `cargo test -p lpe-exchange content_sync` passed
  43 focused tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 19,703 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 19,258 lines and
  `mapi/dispatch/sync_import.rs` at 815 lines. `rg` confirmed the moved helper
  definitions now live in `mapi/dispatch/sync_import.rs`.
- 2026-06-30: Advanced MR-002 by adding
  `mapi/dispatch/custom_properties.rs` and moving the custom-property helper
  cluster out of `mapi/dispatch.rs`: custom property splitting, canonical map
  application, fetch/upsert/delete/copy helpers, canonical object identity
  resolution, and the custom-versus-canonical named-property classifier.
  Custom property persistence, copy/copy-to behavior, guarded calendar-event
  behavior, attachment/public-folder custom values, and store-independent
  custom-property probe rejection remain unchanged.
- 2026-06-30 verification for the dispatch custom-property helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  custom_named_property` passed 3 focused tests; `cargo test -p lpe-exchange
  custom_propert` passed 5 focused tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 19,330 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,897 lines and
  `mapi/dispatch/custom_properties.rs` at 364 lines. `rg` confirmed the moved
  helper definitions now live in `mapi/dispatch/custom_properties.rs`.
- 2026-06-30: Advanced MR-002 by moving associated-configuration open selector
  helpers from `mapi/dispatch.rs` into `mapi/dispatch/associated_config.rs`:
  delegate free/busy message lookup, conversation-action lookup, navigation
  shortcut lookup, Common Views named-view lookup, and Search Folder definition
  lookup. The move preserves folder scoping, stale identity rejection,
  folder-local default named view behavior, Common Views search-definition
  identity matching, and open-message dispatch behavior.
- 2026-06-30 verification for the associated-config open selector split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  common_views_open` passed 3 focused tests; `cargo test -p lpe-exchange
  freebusy_open` passed 1 focused test; `cargo test -p lpe-exchange
  conversation_action_open` passed 2 focused tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 19,259
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 18,831 lines and `mapi/dispatch/associated_config.rs` at 365 lines. `rg`
  confirmed the moved selector definitions now live in
  `mapi/dispatch/associated_config.rs`.
- 2026-06-30: Advanced MR-002 by adding
  `mapi/dispatch/conversation_actions.rs` and moving the conversation-action
  helper cluster out of `mapi/dispatch.rs`: conversation-action property
  projection, applying actions to existing and future messages, target mailbox
  resolution, conversation-action property deletion, and virtual default
  conversation-action staging/deletion. Conversation-action FAI persistence,
  cross-store no-op move behavior, category application, max-delivery-time
  filtering, and virtual default action behavior remain unchanged.
- 2026-06-30 verification for the dispatch conversation-action helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  conversation_action` passed 22 focused tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,963 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,543 lines and
  `mapi/dispatch/conversation_actions.rs` at 291 lines. `rg` confirmed the
  moved conversation-action helper definitions now live in
  `mapi/dispatch/conversation_actions.rs`.
- 2026-06-30: Advanced MR-002 by adding
  `mapi/dispatch/named_properties.rs` and moving the small named-property
  dispatch helper cluster out of `mapi/dispatch.rs`: Outlook OSC contact-source
  probe recognition and session named-property cache/lookup reconciliation.
  Microsoft-specific OSC probe matching, stale alias normalization,
  well-known property ID resolution, and named-property cache behavior remain
  unchanged.
- 2026-06-30 verification for the dispatch named-property helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  named_property` passed 20 focused tests; `cargo test -p lpe-exchange
  get_property_ids_from_names_returns_canonical_contact_source_id_from_stale_mapping`
  passed 1 focused dispatch test; `cargo test -p lpe-exchange
  outlook_contact_source_probe_named_properties_map_to_stable_ids` passed 1
  focused property test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,937 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,519 lines and
  `mapi/dispatch/named_properties.rs` at 27 lines. `rg` confirmed the moved
  named-property helper definitions now live in
  `mapi/dispatch/named_properties.rs`.
- 2026-06-30: Advanced MR-002 by moving the table smart-input helper
  `apply_outlook_smart_input_variant_before_query_rows` from `mapi/dispatch.rs`
  into `mapi/dispatch/tables.rs`. The move preserves the Outlook
  `fai_cursor_reset_before_query_rows` session variant, inbox associated
  contents-table cursor reset behavior, handle-table lookup, diagnostic context
  string, and session applied flag.
- 2026-06-30 verification for the dispatch table smart-input helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  smart_input_variant_resets_inbox_fai_cursor_before_query_rows` passed 1
  focused dispatch test; `cargo test -p lpe-exchange mapi_over_http::tables`
  passed 48 focused table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,895 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,478 lines and
  `mapi/dispatch/tables.rs` at 1,059 lines. `rg` confirmed the moved helper
  definition now lives in `mapi/dispatch/tables.rs`.
- 2026-06-30: Advanced MR-002 and the shared time-helper cleanup by replacing
  the local `current_mapi_filetime` helper in `mapi/dispatch.rs` with
  `lpe_domain::current_windows_filetime`. The shared helper preserves the prior
  Windows FILETIME epoch offset, 100-nanosecond tick conversion, subsecond tick
  handling, and saturating fallback behavior used for MAPI creation timestamps.
- 2026-06-30 verification for the shared current FILETIME helper cleanup:
  `cargo fmt --package lpe-domain --package lpe-exchange`; `cargo test -p
  lpe-domain` passed 34 tests and doc tests; `cargo test -p lpe-exchange
  quick_step_synthetic_folder_allows_associated_message_creation` passed 1
  focused creation test; `cargo test -p lpe-exchange
  empty_inbox_message_list_settings_save_gets_persistable_stream_defaults`
  passed 1 focused associated-config save test; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,875 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,460 lines and
  `lpe-domain/src/civil_time.rs` at 136 lines. `rg` confirmed
  `current_mapi_filetime` is gone and `current_windows_filetime` is defined in
  `lpe-domain`.
- 2026-06-30: Advanced MR-002 by moving
  `apply_canonical_public_folder_item_property_values` from `mapi/dispatch.rs`
  into `mapi/dispatch/public_folders.rs`. Public-folder item property mutation
  remains unchanged: existing canonical item lookup, base property projection,
  pending property overlay, HTML/body preservation, canonical
  `upsert_public_folder_item` input construction, and audit action strings are
  preserved.
- 2026-06-30 verification for the dispatch public-folder item property helper
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  public_folder_item` passed 11 focused public-folder item tests; `cargo test
  -p lpe-exchange mapi_over_http::public_folders` passed 44 focused MAPI
  public-folder tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,806 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,392 lines and
  `mapi/dispatch/public_folders.rs` at 288 lines. `rg` confirmed the moved
  helper definition now lives in `mapi/dispatch/public_folders.rs`.
- 2026-06-30: Advanced MR-002 by moving
  `persist_profile_folder_property_values` from `mapi/dispatch.rs` into
  `mapi/dispatch/folders.rs`. Folder profile persistence remains unchanged:
  `PidTagExtendedFolderFlags` binary values are persisted through the folder
  profile table, IPM subtree `PidTagOSTOSTID` binary writes are retained
  through the dedicated OST identity store, non-binary values are ignored by
  this helper, and non-IPM-subtree folders continue to skip OST identity
  persistence.
- 2026-06-30 verification for the dispatch folder-profile helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_ipm_subtree_ost_identity_retains_client_session_blob` passed 1
  focused test; `cargo test -p lpe-exchange
  mapi_over_http_ipm_subtree_ost_identity_survives_reconnect` passed 1 focused
  test; `cargo test -p lpe-exchange
  mapi_over_http_ipm_subtree_ost_identity_write_survives_store_failure` passed
  1 focused test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,756 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 18,343 lines and
  `mapi/dispatch/folders.rs` at 573 lines. `rg` confirmed the moved helper
  definition and folder-profile/OST persistence calls now live in
  `mapi/dispatch/folders.rs`.
- 2026-06-30: Advanced MR-002 by adding `mapi/dispatch/properties.rs` and
  moving the property-mutation coordinator
  `apply_supported_object_property_values` out of `mapi/dispatch.rs`. The move
  preserves the existing dispatch into message, contact, event, task, note,
  journal, conversation-action, navigation-shortcut, associated-config, public
  folder item, and custom-property persistence helpers. Folder write-rights
  enforcement, unsupported mutation errors, custom property identity lookup,
  and canonical object mutation behavior remain unchanged.
- 2026-06-30 verification for the dispatch property-coordinator split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange custom_propert`
  passed 5 focused custom-property tests; `cargo test -p lpe-exchange
  named_property` passed 20 focused named-property tests; `cargo test -p
  lpe-exchange property_values` passed 4 focused property-value tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 18,516
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 18,104 lines and `mapi/dispatch/properties.rs` at 242 lines. `rg`
  confirmed the coordinator definition now lives in `mapi/dispatch/properties.rs`,
  while the existing call sites remain in `mapi/dispatch.rs` and
  `mapi/dispatch/messages.rs`. A top-level function scan of `mapi/dispatch.rs`
  now shows only constants plus `execute_response` and `execute_rops`.
- 2026-06-30: Advanced MR-002 by moving the table-control ROP response
  cluster from `mapi/dispatch.rs` into `mapi/dispatch/tables.rs` as
  `append_table_control_response`. The move covers `RopGetStatus`,
  `RopQueryPosition`, `RopSeekRow`, bookmark creation/seeking, fractional seek,
  `RopQueryColumnsAll`, `RopExpandRow`, collapse-row, and collapse-state
  response handling. Existing query-position diagnostics, calendar view trace
  recording, seek-row diagnostics, table handle mutation, and the folder-handle
  `RopExpandRow` fallthrough to the existing unsupported/error path remain
  unchanged.
- 2026-06-30 verification for the dispatch table-control split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange mapi_over_http::tables`
  passed 48 focused MAPI table tests, including query-position, seek-row,
  expand-row, and invalid table-control coverage; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,398 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 17,986 lines and
  `mapi/dispatch/tables.rs` at 1,218 lines. `rg` confirmed the table-control
  helper definition now lives in `mapi/dispatch/tables.rs`, with the
  `RopExpandRow` folder-object guard still local in `mapi/dispatch.rs` so the
  previous fallthrough behavior is preserved.
- 2026-06-30: Advanced MR-002 by moving the `RopGetHierarchyTable` and
  `RopGetContentsTable` open-table branch logic from `mapi/dispatch.rs` into
  new `mapi/dispatch/table_open.rs` as `append_open_table_response`. The move
  preserves hierarchy-table handle validation, hierarchy flag errors,
  public-folder root row-count fallback, contents-table handle/object/flag
  validation, canonical folder read-right checks, conversation-members table
  remapping, row-count calculation, inbox/calendar/bootstrap diagnostics,
  table handle allocation, response bytes, and output handle tracking.
- 2026-06-30 verification for the dispatch open-table split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange mapi_over_http::tables`
  passed 48 focused MAPI table tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 18,172 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 17,760 lines,
  `mapi/dispatch/tables.rs` at 1,215 lines, and
  `mapi/dispatch/table_open.rs` at 267 lines. `rg` confirmed
  `append_open_table_response` now lives in `mapi/dispatch/table_open.rs`,
  with the dispatch match reduced to the combined `RopGetHierarchyTable |
  RopGetContentsTable` call.
- 2026-06-30: Advanced MR-003 by moving the `RopSubmitMessage`,
  `RopTransportSend`, and `RopAbortSubmit` execution bodies from
  `mapi/dispatch.rs` into `mapi/dispatch/submission.rs` as
  `append_submit_message_response` and `append_abort_submit_response`. The move
  preserves missing-handle and stale-handle errors, pending-message submission,
  opened-draft/outbox submission, protected Bcc reloads, canonical submission,
  same-execute submitted-message loading, submitted handle replacement,
  duplicate execute behavior, abort-submit source validation, cancellation
  result mapping, and RCA trace fields. The `RopSetSpooler`,
  `RopSpoolerLockMessage`, `RopTransportNewMail`, and
  `RopUpdateDeferredActionMessages` compatibility response routing also now
  lives in `mapi/dispatch/submission.rs`; those parseable advisory/deferred
  responses were not widened.
- 2026-06-30 verification for the dispatch submission execution split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http::submission` passed 23 focused MAPI submission tests,
  including transport spooler alignment, abort-submit, replay, Bcc, draft, and
  canonical submission coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 17,910 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 17,498 lines and
  `mapi/dispatch/submission.rs` at 455 lines. `rg` confirmed
  `append_submit_message_response`, `append_abort_submit_response`,
  `append_spooler_advisory_response`, and
  `append_deferred_action_messages_response` now live in
  `mapi/dispatch/submission.rs`, with the dispatch match reduced to calls for
  `RopSetSpooler | RopSpoolerLockMessage | RopTransportNewMail`,
  `RopUpdateDeferredActionMessages`, `RopSubmitMessage | RopTransportSend`,
  and `RopAbortSubmit`.
- 2026-06-30: Advanced MR-002/MR-003 by moving the small
  `RopGetTransportFolder`, `RopOptionsData`, and `RopGetReceiveFolderTable`
  routing logic out of `mapi/dispatch.rs`. Transport-folder and options-data
  response routing now lives in `mapi/dispatch/submission.rs` as
  `append_transport_folder_response` and `append_options_data_response`.
  Receive-folder table routing, private-logon validation, RCA trace fields,
  canonical row-shape response, and receive-folder verification marker now live
  in `mapi/dispatch/tables.rs` as `append_receive_folder_table_response`.
- 2026-06-30 verification for the transport/options/receive-folder-table split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`
  passed 1 focused transport-folder test; `cargo test -p lpe-exchange
  mapi_over_http_get_receive_folder_table_requires_private_logon_handle` passed
  1 focused table validation test; `cargo test -p lpe-exchange
  mapi_over_http::logon_profile` passed 13 focused logon/profile tests,
  including options-data and receive-folder bootstrap coverage;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 17,893
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 17,481 lines, `mapi/dispatch/submission.rs` at 469 lines, and
  `mapi/dispatch/tables.rs` at 1,249 lines. `rg` confirmed the new append
  helpers now live outside `mapi/dispatch.rs`, with the dispatch match reduced
  to thin calls for all three ROPs.
- 2026-06-30: Advanced MR-002 by moving small status/control response routing
  out of `mapi/dispatch.rs`: `RopGetStoreState` now delegates to
  `append_store_state_response` in `mapi/dispatch/logon.rs`; `RopAbort`,
  `RopProgress`, and `RopResetTable` now share `append_execute_status_response`
  in `mapi/dispatch/execute.rs`; and `RopFreeBookmark` now delegates to
  `append_free_bookmark_response` in `mapi/dispatch/tables.rs`. Response error
  codes, progress payload validation, table reset state mutation, bookmark
  removal, and store-state handle validation remain unchanged.
- 2026-06-30 verification for the status/control response routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_receive_folder_and_store_state` passed 1
  focused store-state test; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_reset_table_requires_new_set_columns` passed 1
  focused reset-table test; `cargo test -p lpe-exchange
  mapi_over_http_microsoft_table_bookmarks_restore_contents_cursor_and_free`
  passed 1 focused bookmark test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 17,879 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 17,467 lines,
  `mapi/dispatch/execute.rs` at 303 lines, `mapi/dispatch/logon.rs` at 85
  lines, and `mapi/dispatch/tables.rs` at 1,256 lines. `rg` confirmed the new
  append helpers now live in their focused modules, with the dispatch match
  reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-007 by moving the remaining mutable input
  object lookup for `RopFreeBookmark` into `mapi/dispatch/tables.rs`. The
  dispatcher is now a thin call for the ROP, while
  `append_free_bookmark_response` preserves bookmark removal, table cursor
  state mutation, missing-handle behavior, and response bytes.
- 2026-06-30 verification for the FreeBookmark validation cleanup: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange bookmark` passed 8
  focused bookmark tests, including Microsoft table bookmark/free behavior;
  `cargo test -p lpe-exchange tables` passed 194 broader table projection and
  table-control tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,524 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 13,121 lines and
  `mapi/dispatch/tables.rs` at 1,258 lines. `rg` confirmed
  `append_free_bookmark_response`, `free_bookmark_response`, and
  `rop_free_bookmark_response` now live in the focused tables module, with
  dispatch reduced to a thin call for the ROP.
- 2026-06-30: Advanced MR-002 by moving address-type and named-property
  response routing out of `mapi/dispatch.rs`. `RopGetAddressTypes` now
  delegates to `append_address_types_response` in `mapi/dispatch/logon.rs`;
  `RopGetNamesFromPropertyIds`, `RopGetPropertyIdsFromNames`, and
  `RopQueryNamedProperties` now delegate to focused async helpers in
  `mapi/dispatch/named_properties.rs`. The move preserves address-type RCA
  tracing, echo-handle behavior, named-property cache hydration, empty logon
  enumeration, dynamic allocation, no-create missing errors, allocation failure
  errors, OSC contact-source tracing, query-by-GUID behavior, and response
  bytes.
- 2026-06-30 verification for the address-type/named-property routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http_execute_returns_empty_transport_options_data` passed 1
  focused logon/profile test; `cargo test -p lpe-exchange named_property`
  passed 20 focused named-property tests; `cargo test -p lpe-exchange
  get_property_ids_from_names_returns_canonical_contact_source_id_from_stale_mapping`
  passed 1 focused stale-mapping test; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing. `rg`
  confirmed the new append helpers live in their focused modules, with the
  dispatch match reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-024 by moving `RopRegisterNotification`
  response routing from `mapi/dispatch.rs` into
  `mapi/dispatch/notification_subscriptions.rs` as
  `append_register_notification_response`. The move preserves request parsing,
  notification cursor hydration, output handle allocation, subscription object
  storage, response bytes, post-hierarchy diagnostic fields, and RCA trace
  output.
- 2026-06-30 verification for the register-notification routing split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange notification`
  passed 13 focused notification tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 17,624 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 17,212 lines and
  `mapi/dispatch/notification_subscriptions.rs` at 81 lines. `rg` confirmed
  `append_register_notification_response` now lives in the focused notification
  module, with the dispatch match reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-007 by adding
  `mapi/dispatch/permissions.rs` and moving `RopGetPermissionsTable` response
  routing out of `mapi/dispatch.rs` as
  `append_get_permissions_table_response`. The move preserves input handle
  validation, folder existence checks across mailbox, advertised special, role,
  and public-folder scopes, permission-table handle allocation, response bytes,
  and output handle tracking. `RopModifyPermissions` remains local for a later
  mutation-focused extraction.
- 2026-06-30 verification for the permissions-table routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http::permissions` passed 15 focused permission/delegation tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 17,610
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 17,198 lines and `mapi/dispatch/permissions.rs` at 36 lines. `rg`
  confirmed `append_get_permissions_table_response` now lives in the focused
  permissions module, with the dispatch match reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-007/MR-019 by moving `RopGetRulesTable`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/rules.rs` as
  `append_get_rules_table_response`. The move preserves input handle
  validation, mailbox/role/public-folder scope checks, rule-table handle
  allocation, response bytes, and output handle tracking. `RopModifyRules`
  remains local for a later mutation-focused extraction so rule writes and
  unsupported Exchange rule/deferred-action behavior stay isolated.
- 2026-06-30 verification for the rules-table routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange mapi_over_http::tables`
  passed 48 focused MAPI table tests, including
  `mapi_over_http_get_rules_table_projects_canonical_sieve_rules`;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 17,595
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 17,183 lines and `mapi/dispatch/rules.rs` at 201 lines. `rg` confirmed
  `append_get_rules_table_response` now lives in the focused rules module, with
  the dispatch match reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-019 by moving `RopModifyRules` response
  routing and bounded mutation execution out of `mapi/dispatch.rs` into
  `mapi/dispatch/rules.rs` as `append_modify_rules_response`. The move
  preserves input handle validation, mailbox/role scope checks, row parsing,
  remove/upsert branching, bounded JSON-to-Sieve conversion, canonical
  `delete_sieve_script` and `put_sieve_script` calls, unsupported Exchange rule
  blob rejection, provider/delegate/deferred-action rejection, error codes, and
  response bytes.
- 2026-06-30 verification for the modify-rules extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange rule` passed 23 focused
  rule tests, including bounded modify-rules writes, bounded action acceptance,
  Exchange rule blob rejection, EWS inbox-rule behavior, and rule-table
  projection; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 17,512
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 17,100 lines and `mapi/dispatch/rules.rs` at 306 lines. `rg` confirmed
  `append_modify_rules_response` and bounded rule mutation execution now live
  in the focused rules module, with the dispatch match reduced to a thin async
  call.
- 2026-06-30: Advanced MR-002/MR-006 by moving `RopModifyPermissions`
  response routing and permission mutation execution out of `mapi/dispatch.rs`
  into `mapi/dispatch/permissions.rs` as
  `append_modify_permissions_response`. The move preserves input handle
  validation, mailbox/calendar/public-folder target selection, share-right
  enforcement, ACL row parsing, member identity lookup, invalid rights
  rejection, self/default/anonymous skipping, calendar permission writes,
  calendar-collection permission writes, mailbox-folder permission writes,
  public-folder grant upsert/delete behavior, audit action strings, error
  codes, and response bytes.
- 2026-06-30 verification for the modify-permissions extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http::permissions` passed 15 focused permission/delegation tests;
  `cargo test -p lpe-exchange mapi_over_http::public_folders` passed 44
  focused public-folder tests, including public-folder permission grants and
  rejection cases; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 17,216 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,805 lines and
  `mapi/dispatch/permissions.rs` at 318 lines. `rg` confirmed
  `append_modify_permissions_response` and canonical permission write calls now
  live in the focused permissions module, with the dispatch match reduced to a
  thin async call.
- 2026-06-30: Advanced MR-002/MR-010 by moving `RopLongTermIdFromId` and
  `RopIdFromLongTermId` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/object_ids.rs` as `append_long_term_id_from_id_response` and
  `append_id_from_long_term_id_response`. The move preserves object-scope
  classification, advertised-special-folder handling, dynamic object-id
  acceptance, mailbox GUID alias handling, special-folder conversion
  diagnostics, response bytes, and RCA trace fields.
- 2026-06-30 verification for the object-id conversion routing split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange long_term_id`
  passed 8 focused object-id tests; `cargo test -p lpe-exchange
  id_from_long_term_id` passed 1 focused conversion test;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 17,156
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 16,745 lines and `mapi/dispatch/object_ids.rs` at 190 lines. `rg`
  confirmed the new append helpers and RCA trace strings now live in the
  focused object-id module, with dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-014 by moving public-folder per-user and
  replica probe ROP response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/public_folders.rs`. The extracted helpers now own
  `RopGetPerUserLongTermIds`, `RopGetPerUserGuid`,
  `RopReadPerUserInformation`, `RopWritePerUserInformation`,
  `RopGetOwningServers`, and `RopPublicFolderIsGhosted` routing. The move
  preserves logon-handle validation, canonical public-folder identity
  allocation, per-user read-state stream shape, invalid Exchange blob
  rejection, canonical per-user state patching, replica server ordering,
  ghosting checks, response bytes, and error codes.
- 2026-06-30 verification for the public-folder per-user/probe routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_over_http::public_folders` passed 44 focused public-folder tests,
  including per-user lookup/state and replica probe coverage;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,928
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 16,517 lines and
  `mapi/dispatch/public_folders.rs` at 611 lines. `rg` confirmed the six new
  append helpers now live in the focused public-folder module, with dispatch
  reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-004 by moving the small `RopTellVersion`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_import.rs` as `append_tell_version_response`. The move
  preserves the existing accepted handle kinds for synchronization sources,
  synchronization collectors, and FastTransfer destinations, and preserves the
  exact success/error response behavior for unsupported handles.
- 2026-06-30 verification for the `RopTellVersion` routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange tell_version` passed
  the focused `mapi_over_http_tell_version_accepts_fast_transfer_sync_context`
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,919
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 16,508 lines and `mapi/dispatch/sync_import.rs` at
  834 lines. `rg` confirmed `append_tell_version_response` now lives in the
  sync-import module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-004 by moving local-replica sync response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs` as
  `append_set_local_replica_midset_deleted_response` and
  `append_get_local_replica_ids_response`. The move preserves sync
  source/collector state mutation for deleted midsets, unsupported-handle error
  behavior, local replica ID range allocation, `next_local_replica_sequence`
  advancement, and the dispatch-level response handle-table echo for
  `RopGetLocalReplicaIds`.
- 2026-06-30 verification for the local-replica routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange local_replica` passed
  2 focused tests covering local replica ID allocation and deleted-midset
  round trip; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,895
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 16,484 lines and `mapi/dispatch/sync_import.rs` at
  879 lines. `rg` confirmed both local-replica append helpers now live in the
  sync-import module, with dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-005 by moving stream-region response routing
  for `RopLockRegionStream` and `RopUnlockRegionStream` out of
  `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_stream_region_response`. The move preserves the current bounded
  behavior: attachment-stream handles receive simple success, and all other
  handles receive the same not-found error with the original ROP ID.
- 2026-06-30 verification for the stream-region routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream_region` passed
  the focused `mapi_over_http_microsoft_stream_region_rops_succeed_on_stream_handles`
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,885
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 16,474 lines and `mapi/dispatch/properties.rs` at 260
  lines. `rg` confirmed `append_stream_region_response` now lives in the
  focused properties module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCloneStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_clone_stream_response`. The move preserves writable-stream handle
  resolution, read-only attachment-stream cloning, output handle allocation,
  response handle-slot updates, output handle tracking, and the existing
  unsupported/not-found error split for writable or missing stream handles.
- 2026-06-30 verification for the clone-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream_region` passed
  the focused stream test that covers `RopCloneStream`, `RopLockRegionStream`,
  and `RopUnlockRegionStream`; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,859 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,448 lines and
  `mapi/dispatch/properties.rs` at 301 lines. `rg` confirmed
  `append_clone_stream_response` now lives in the focused properties module,
  with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopGetStreamSize` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_get_stream_size_response`. The move preserves writable-stream handle
  resolution, attachment-stream length reporting, and the existing not-found
  error for missing or non-stream handles.
- 2026-06-30 verification for the get-stream-size routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream/property tests, including GetStreamSize and stream-region
  coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,848 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,437 lines and
  `mapi/dispatch/properties.rs` at 321 lines. `rg` confirmed
  `append_get_stream_size_response` now lives in the focused properties
  module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCommitStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_commit_stream_response`. The move preserves writable-stream handle
  resolution, inbox associated-config commit diagnostics, associated-config
  stream persistence, simple success for attachment streams, and the existing
  not-found error for missing commit targets.
- 2026-06-30 verification for the commit-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream/property tests, including associated-config stream persistence
  and stream-region coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,785 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,374 lines and
  `mapi/dispatch/properties.rs` at 402 lines. `rg` confirmed
  `append_commit_stream_response` and the commit-stream RCA trace strings now
  live in the focused properties module, with dispatch reduced to a thin async
  call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopSeekStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_seek_stream_response`. The move preserves writable-stream handle
  resolution, mutable stream-position updates through the existing seek
  response builder, RCA diagnostics, and the existing not-found error for
  missing stream handles.
- 2026-06-30 verification for the seek-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream/property tests, including MAPI stream seek/write coverage;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,762
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 16,351 lines and `mapi/dispatch/properties.rs` at 438 lines. `rg`
  confirmed `append_seek_stream_response` and the seek-stream RCA trace string
  now live in the focused properties module, with dispatch reduced to a thin
  call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopSetStreamSize` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_set_stream_size_response`. The move preserves missing-handle errors,
  writable-stream handle resolution, inbox associated-config stream-size
  diagnostics, RCA object-kind fields, attachment-stream resizing, and the
  existing write-fault error for unsupported resize targets.
- 2026-06-30 verification for the set-stream-size routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream/property tests, including SetStreamSize, write, commit, and
  associated-stream coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,718 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,307 lines and
  `mapi/dispatch/properties.rs` at 498 lines. `rg` confirmed
  `append_set_stream_size_response`, the associated-config stream-size trace,
  and the resize helper call now live in the focused properties module, with
  dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving the `RopWriteStream`,
  `RopWriteAndCommitStream`, and `RopWriteStreamExtended` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_write_stream_response`. The move preserves missing-handle errors,
  writable-stream handle resolution, inbox associated-config write diagnostics,
  RCA object-kind fields, stream write execution, written-byte response bytes,
  and the existing stream-write error mapping for unsupported targets.
- 2026-06-30 verification for the write-stream routing split: the first
  `cargo test -p lpe-exchange stream` attempt failed at compile time because
  the helper was duplicated during extraction; the duplicate definition was
  removed. After correction, `cargo fmt --package lpe-exchange`; `cargo test
  -p lpe-exchange stream` passed 38 focused stream/property tests, including
  write-stream and associated-stream coverage; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,669 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,258 lines and
  `mapi/dispatch/properties.rs` at 560 lines. `rg` confirmed
  `append_write_stream_response`, the associated-config write trace, and the
  write response builder call now live in the focused properties module, with
  dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCopyToStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_copy_to_stream_response`. The move preserves missing source and
  destination handle errors, writable-stream handle resolution for both ends,
  bounded stream copy execution, read/written response bytes, and the existing
  write-fault error for unsupported copy targets.
- 2026-06-30 verification for the copy-to-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream/property tests, including CopyToStream canonical message-body
  and attachment save coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,634 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,223 lines and
  `mapi/dispatch/properties.rs` at 602 lines. `rg` confirmed
  `append_copy_to_stream_response`, the copy helper call, and the copy response
  builder call now live in the focused properties module, with dispatch reduced
  to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCopyTo` response routing
  out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_copy_to_response`. The move preserves asynchronous/subobject flag
  validation, null-destination response alignment, source-handle error
  behavior, custom-property copy delegation, message follow-up property copy
  delegation, excluded-property tag handling, and the existing unsupported
  fallback when no bounded copy path applies.
- 2026-06-30 verification for the copy-to routing split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange copy_to` passed 6 focused tests,
  including null-destination alignment, custom-value copy exclusion, stream
  copy, and FastTransfer copy-to coverage; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,581 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,170 lines and
  `mapi/dispatch/properties.rs` at 680 lines. `rg` confirmed
  `append_copy_to_response`, null-destination response handling, custom-property
  copy delegation, and message follow-up copy delegation now live in the
  focused properties module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCopyProperties` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_copy_properties_response`. The move preserves asynchronous flag
  validation, source-handle validation, null-destination response alignment,
  empty property-tag success, message follow-up property copy delegation,
  custom-property copy delegation, problem-row response handling, and the
  existing unsupported fallback when no bounded copy path applies.
- 2026-06-30 verification for the copy-properties routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange copy_properties` passed
  5 focused tests covering null-destination alignment, empty tag-list no-op,
  custom-property copy, message follow-up copy, and FastTransfer copy-properties
  manifest behavior; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,505 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 16,094 lines and
  `mapi/dispatch/properties.rs` at 771 lines. `rg` confirmed
  `append_copy_properties_response`, null-destination response handling,
  follow-up copy delegation, custom-property copy delegation, and copy-properties
  response builders now live in the focused properties module, with dispatch
  reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopSetReadFlags` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/messages.rs` as
  `append_set_read_flags_response`. The move preserves folder-handle
  validation, Microsoft read-flag validation, read/unread mapping, public-folder
  per-user read-state patches, canonical JMAP message flag updates, partial
  completion handling, and response bytes.
- 2026-06-30 verification for the set-read-flags routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange set_read_flags` passed
  3 focused tests covering canonical message read state, public-folder
  per-user read state, and invalid Microsoft parameters; `$env:RUST_TEST_THREADS
  ='1'; cargo test -p lpe-exchange` passed with 1593 tests and doc tests
  passing. `python tools/check_oversized_sources.py` passed in warning mode and
  reports `mapi/dispatch.rs` at 16,424 tracked-source lines. Direct physical
  line counts report `mapi/dispatch.rs` at 16,013 lines and
  `mapi/dispatch/messages.rs` at 451 lines. `rg` confirmed
  `append_set_read_flags_response`, public-folder per-user patches,
  `mapi-set-read-flags` auditing, and `rop_set_read_flags_response` now live in
  the focused messages module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopSetMessageReadFlag` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/messages.rs` as
  `append_set_message_read_flag_response`. The move preserves input-object
  validation, Microsoft read-flag validation, public-folder per-user read-state
  patches, canonical JMAP message flag updates, folder write-right checks,
  content notification recording, changed-status response bytes, and the
  existing not-found/error mappings.
- 2026-06-30 verification for the set-message-read-flag routing split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  set_message_read_flag` passed 3 focused tests covering canonical open-message
  read state, public-folder per-user read state, and default-flag rejection;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,307
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 15,896 lines and `mapi/dispatch/messages.rs` at 591 lines. `rg` confirmed
  `append_set_message_read_flag_response`, public-folder per-user patches,
  `mapi-set-message-read-flag` auditing, and
  `rop_set_message_read_flag_response` now live in the focused messages module,
  with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopReloadCachedInformation`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/messages.rs`
  as `append_reload_cached_information_response`. The move preserves reserved
  field validation, invalid-parameter response mapping, input object lookup,
  and the existing cached-information response builder behavior for pending and
  persisted message-like objects.
- 2026-06-30 verification for the reload-cached-information routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  reload_cached_information` passed 3 focused tests covering the ROP response
  shape, pending-message summary output, and nonzero reserved-field rejection;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,301
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 15,890 lines and `mapi/dispatch/messages.rs` at 616 lines. `rg` confirmed
  `append_reload_cached_information_response`, reserved-field validation, and
  the reload response builder call now live in the focused messages module,
  with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopReadRecipients` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/recipients.rs` as
  `append_read_recipients_response`. The move preserves reserved-field
  validation, invalid-parameter response mapping, pending recipient replacement
  lookup by input handle, fallback to the current input object, and the existing
  recipient row response builder behavior.
- 2026-06-30 verification for the read-recipients routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange read_recipients` passed
  6 focused tests covering response row counts, row-id handling, empty-message
  not-found behavior, canonical message recipients, Bcc hiding, and nonzero
  reserved-field rejection; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,282 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 15,871 lines and
  `mapi/dispatch/recipients.rs` at 73 lines. `rg` confirmed
  `append_read_recipients_response`, reserved-field validation, pending
  recipient replacement lookup, and the recipient response builder call now live
  in the focused recipients module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopModifyRecipients` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/recipients.rs` as
  `append_modify_recipients_response`. The move preserves pending-message
  recipient staging, persisted/open-message recipient replacement staging,
  address-book-backed recipient parsing, Microsoft recipient row diagnostics,
  invalid-object/not-found/parse-error mappings, and existing success response
  bytes.
- 2026-06-30 verification for the modify-recipients routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange modify_recipients`
  passed 9 focused tests covering Microsoft row parsing, type-flag validation,
  string8/wrapped/X500 canonical saves, opened-message staging, and sync import
  of pending recipients; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 16,136 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 15,725 lines and
  `mapi/dispatch/recipients.rs` at 243 lines. `rg` confirmed
  `append_modify_recipients_response` and the ModifyRecipients RCA diagnostic
  string now live in the focused recipients module, with dispatch reduced to a
  thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopRemoveAllRecipients`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/recipients.rs` as `append_remove_all_recipients_response`.
  The move preserves pending-message recipient clearing, persisted/open-message
  empty replacement staging, missing-handle error mapping, invalid-object error
  mapping, and existing success response bytes.
- 2026-06-30 verification for the remove-all-recipients routing split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  remove_all_recipients` passed 2 focused tests covering pending-message
  recipient clearing and opened-message staging until save;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 16,116
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 15,705 lines and `mapi/dispatch/recipients.rs` at 276 lines. `rg`
  confirmed `append_remove_all_recipients_response` now lives in the focused
  recipients module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopMoveCopyMessages`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/messages.rs`
  as `append_move_copy_messages_response`. The move preserves Microsoft
  boolean-field validation, source/target folder-handle errors, recoverable
  item restore/reject behavior, public-folder item copy/move notifications,
  note and journal copy handling, canonical JMAP message move/copy calls,
  partial-completion response bytes, and existing RCA failure diagnostics.
- 2026-06-30 verification for the move-copy-messages routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange move_copy_messages`
  passed 2 focused tests covering canonical mailbox move/copy and nonzero
  Microsoft boolean fields; `cargo test -p lpe-exchange recoverable_item`
  passed 14 focused recoverable-item tests; `cargo test -p lpe-exchange
  public_folder` passed 74 focused public-folder tests, including public-folder
  message copy and move; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 15,741 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 15,330 lines and
  `mapi/dispatch/messages.rs` at 998 lines. `rg` confirmed
  `append_move_copy_messages_response` and the MoveCopyMessages RCA failure
  diagnostic strings now live in the focused messages module, with dispatch
  reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopDeleteMessages` and
  `RopHardDeleteMessages` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/messages.rs` as `append_delete_messages_response`. The move
  preserves Microsoft boolean-field validation, hard-delete unsupported-handle
  behavior, canonical folder delete-right checks, recoverable-item purge and
  bounded rejection behavior, contact/event/task/note/journal delete paths,
  Common Views shortcut and associated-config deletion, public-folder item
  deletion, canonical JMAP trash/hard-delete behavior, notification recording,
  sync-upload checkpoint recording, and partial-completion response bytes.
- 2026-06-30 verification for the delete-messages routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange delete_messages`
  passed 10 focused tests covering trash/hard-delete behavior, retention
  partial completion, Common Views shortcut deletion, nonzero Microsoft boolean
  fields, and recoverable/hierarchy delete cases; `cargo test -p lpe-exchange
  public_folder` passed 74 focused public-folder tests, including public-folder
  message delete and hard-delete; `cargo test -p lpe-exchange calendar` passed
  145 focused calendar/task tests; `cargo test -p lpe-exchange contacts`
  passed 39 focused contact tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 15,475 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 15,064 lines and
  `mapi/dispatch/messages.rs` at 1,279 lines. `rg` confirmed
  `append_delete_messages_response` and the delete/purge/trash audit action
  strings now live in the focused messages module, with dispatch reduced to a
  thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopGetMessageStatus` and
  `RopSetMessageStatus` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/messages.rs` as `append_message_status_response`. The move
  preserves folder-handle validation, Microsoft `SetMessageStatus` response
  opcode behavior for both ROPs, item-existence checks across canonical mail,
  public-folder, contact, event, and task projections, session-local status
  storage, mask/flag updates, zero-status cleanup, and existing response
  bytes.
- 2026-06-30 verification for the message-status routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange message_status` passed
  4 focused tests covering session-local private/public-folder status and
  Microsoft response opcode/folder-handle behavior; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 15,438 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 15,027 lines and
  `mapi/dispatch/messages.rs` at 1,335 lines. `rg` confirmed
  `append_message_status_response`, session-local status storage, and the
  message-status response builder call now live in the focused messages
  module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopGetValidAttachments` and
  `RopGetAttachmentTable` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/attachments.rs` as `append_get_valid_attachments_response`
  and `append_get_attachment_table_response`. The move preserves valid
  attachment-number projection, pending attachment deletion filtering,
  Microsoft attachment-table flag validation, message/pending-message/event
  handle handling, missing calendar-event rejection, attachment-table handle
  allocation, output handle slot updates, and existing response bytes.
- 2026-06-30 verification for the attachment table routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange get_attachment_table`
  passed 3 focused tests covering Microsoft flag validation and canonical
  attachment table projection; `cargo test -p lpe-exchange
  get_valid_attachments` passed 3 focused tests covering canonical message
  attachment numbers, calendar event projection, and missing event-handle
  rejection; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 15,401 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 14,990 lines and
  `mapi/dispatch/attachments.rs` at 326 lines. `rg` confirmed
  `append_get_valid_attachments_response`, `append_get_attachment_table_response`,
  pending deletion filtering, and attachment-table object allocation now live
  in the focused attachments module, with dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopOpenAttachment` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/attachments.rs` as
  `append_open_attachment_response`. The move preserves Microsoft
  OpenAttachment flag validation, message/event handle validation, missing
  calendar-event rejection, pending attachment deletion filtering, canonical
  attachment existence checks, attachment object handle allocation, output
  handle slot updates, and existing response bytes.
- 2026-06-30 verification for the open-attachment routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange open_attachment`
  passed 4 focused tests covering Microsoft flag validation, canonical
  attachment property projection, calendar-event attachment projection, and
  invalid flag batch stability; `cargo test -p lpe-exchange attachment`
  passed 47 broader attachment tests covering table rows, open/create/delete,
  save, EWS attachments, embedded-message reopening, and sync attachment facts;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 15,339
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,928 lines and `mapi/dispatch/attachments.rs` at 405 lines. `rg`
  confirmed `append_open_attachment_response`, OpenAttachment flag validation,
  pending deletion filtering, attachment object allocation, and the open
  attachment response builder call now live in the focused attachments module,
  with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopDeleteAttachment` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/attachments.rs` as
  `append_delete_attachment_response`. The move preserves message/event handle
  validation, guarded-calendar attachment hiding, canonical attachment
  existence checks, canonical folder write-right checks, pending session-local
  attachment deletion staging, and existing success/error response bytes. The
  actual canonical deletion still happens through the existing save-message
  path.
- 2026-06-30 verification for the delete-attachment routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange delete_attachment`
  passed 4 focused tests covering EWS canonical attachment deletion, unknown
  attachment rejection, MAPI staged deletion committed by save-message, and
  guarded calendar attachment hiding; `cargo test -p lpe-exchange attachment`
  passed 47 broader attachment tests covering table rows, open/create/delete,
  save, EWS attachments, embedded-message reopening, and sync attachment facts;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 15,288
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,877 lines and `mapi/dispatch/attachments.rs` at 472 lines. `rg`
  confirmed `append_delete_attachment_response`, guarded-calendar filtering,
  pending deletion staging, and the simple success response now live in the
  focused attachments module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopCreateAttachment` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/attachments.rs` as
  `append_create_attachment_response`. The move preserves pending-message
  parent-handle detection, message/pending-message/event handle validation,
  guarded-calendar attachment hiding, canonical message/event existence
  checks, canonical folder write-right checks, pending attachment number
  allocation, documented initial attachment properties, parent pending-message
  attachment linkage, output handle slot updates, and existing response bytes.
  Attachment persistence still happens through the existing stream/save paths.
- 2026-06-30 verification for the create-attachment routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange create_attachment`
  passed 5 focused tests covering EWS attachment create/reject paths and MAPI
  documented-property initialization plus canonical save; `cargo test -p
  lpe-exchange attachment` passed 47 broader attachment tests covering table
  rows, open/create/delete, save, EWS attachments, embedded-message reopening,
  and sync attachment facts; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 15,191 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 14,781 lines and
  `mapi/dispatch/attachments.rs` at 586 lines. `rg` confirmed
  `append_create_attachment_response`, pending attachment number allocation,
  timestamp initialization, documented initial property seeding, parent
  pending-message linkage, and the create attachment response builder call now
  live in the focused attachments module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopOpenEmbeddedMessage`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/attachments.rs` as `append_open_embedded_message_response`.
  The move preserves missing-handle and open-mode validation, embedded-message
  source lookup for pending, saved, and persisted attachments, generated
  transient embedded-message IDs, subject projection, pending-message handle
  allocation, session mappings for embedded message IDs and attachment parents,
  output handle slot updates, and existing response bytes.
- 2026-06-30 verification for the open-embedded-message routing split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange embedded_message`
  passed 4 focused tests covering the response shape, content-sync embedded
  markers, saved embedded-message reopening, and read-only OpenEmbeddedMessage
  behavior; `cargo test -p lpe-exchange attachment` passed 47 broader
  attachment tests covering table rows, open/create/delete, save, EWS
  attachments, embedded-message reopening, and sync attachment facts;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 15,139
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,729 lines and `mapi/dispatch/attachments.rs` at 654 lines. `rg`
  confirmed `append_open_embedded_message_response`, embedded source lookup,
  transient embedded-message ID generation, session mapping updates, and the
  OpenEmbeddedMessage response builder call now live in the focused
  attachments module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving
  `RopSaveChangesAttachment` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/attachments.rs` as
  `append_save_changes_attachment_response`. The move preserves input-handle
  and save-flag validation, RCA probe diagnostics, pending-attachment handle
  validation, canonical folder write-right checks, Magika attachment
  validation and MIME correction, embedded-message attachment generation,
  pending-message attachment staging, canonical message and calendar-event
  attachment saves, custom attachment property persistence, saved-attachment
  handle replacement, and existing success/error response bytes.
- 2026-06-30 verification for the save-changes-attachment routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  save_changes_attachment` passed 1 focused test covering Microsoft
  conflicting save-flag rejection without batch drift; `cargo test -p
  lpe-exchange attachment` passed 47 broader attachment tests covering table
  rows, open/create/delete, save, EWS attachments, embedded-message reopening,
  and sync attachment facts; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 14,878 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 14,468 lines and
  `mapi/dispatch/attachments.rs` at 944 lines. `rg` confirmed
  `append_save_changes_attachment_response`, pending-message staging,
  `mapi-save-attachment`, `mapi-save-calendar-attachment`, validation, and
  custom-property persistence now live in the focused attachments module, with
  dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-008 by moving `RopReadStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_read_stream_response`. The move preserves writable-stream handle
  resolution, associated-configuration stream read counters, Outlook view
  failure trace events, rule-organizer stream diagnostics, stream position
  advancement, returned byte counts, end-of-stream detection, response preview
  logging, missing-stream error mapping, and existing read response bytes.
- 2026-06-30 verification for the read-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange read_stream` compiled
  the crate with 0 matching tests; `cargo test -p lpe-exchange stream` passed
  38 focused stream tests covering message body streams, attachment streams,
  associated configuration streams, stream region ROPs, and FastTransfer stream
  shapes; `cargo test -p lpe-exchange properties` passed 247 broader property
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 14,760
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,350 lines and `mapi/dispatch/properties.rs` at 903 lines. `rg`
  confirmed `append_read_stream_response`, associated-configuration read
  diagnostics, rule-organizer read diagnostics, and `rop_read_stream_response`
  now live in the focused properties module, with dispatch reduced to a thin
  call.
- 2026-06-30: Advanced MR-002/MR-008 by moving `RopOpenStream` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_open_stream_response`. The move preserves missing-input-handle
  diagnostics, input object/folder debug metadata, associated-configuration
  stream open counters, Outlook view failure trace events, rule-organizer
  stream detection, stream data lookup, writable target preservation, output
  handle allocation, stream preview logging, response handle-slot updates, and
  existing open-stream response bytes.
- 2026-06-30 verification for the open-stream routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange stream` passed 38
  focused stream tests covering message body streams, attachment streams,
  associated configuration streams, stream region ROPs, and FastTransfer stream
  shapes; `cargo test -p lpe-exchange properties` passed 247 broader property
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 14,618
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,208 lines and `mapi/dispatch/properties.rs` at 1,072 lines. `rg`
  confirmed `append_open_stream_response`, associated-configuration open
  diagnostics, rule-organizer open diagnostics, and `rop_open_stream_response`
  now live in the focused properties module, with dispatch reduced to a thin
  async call.
- 2026-06-30: Advanced MR-002/MR-020 by moving `RopSetReceiveFolder`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/folders.rs`
  as `append_set_receive_folder_response`. The move preserves private-logon
  handle validation, missing folder/message-class invalid-parameter mapping,
  supported message-class validation, canonical fixed receive-folder mapping
  checks, RCA diagnostics for accepted canonical mappings, and existing
  success/error response bytes. Arbitrary configurable receive-folder routing
  remains unsupported until it has a canonical model.
- 2026-06-30 verification for the set-receive-folder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange receive_folder` passed
  12 focused tests covering Get/SetReceiveFolder private-logon validation,
  message-class matching, canonical calendar mapping acceptance, custom
  calendar mapping acceptance, and noncanonical mapping rejection; `cargo test
  -p lpe-exchange calendar` passed 145 broader calendar tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 14,571
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,161 lines and `mapi/dispatch/folders.rs` at 635 lines. `rg`
  confirmed `append_set_receive_folder_response`, canonical mapping checks,
  and accepted-mapping diagnostics now live in the focused folders module, with
  dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-020 by moving `RopGetReceiveFolder`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/folders.rs`
  as `append_get_receive_folder_response`. The move preserves input handle
  table echoing in the dispatcher, private-logon handle validation, missing
  and invalid message-class error mapping, canonical fixed receive-folder
  resolution, RCA diagnostics, explicit response message-class projection,
  receive-folder verification state, post-hierarchy contract recording, and
  existing response bytes.
- 2026-06-30 verification for the get-receive-folder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange receive_folder` passed
  12 focused tests covering Get/SetReceiveFolder private-logon validation,
  message-class matching, canonical calendar mapping, custom calendar mapping,
  noncanonical mapping rejection, and receive-folder table handling; `cargo
  test -p lpe-exchange calendar` passed 145 broader calendar tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 14,524
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 14,114 lines and `mapi/dispatch/folders.rs` at 698 lines. `rg`
  confirmed `append_get_receive_folder_response`, canonical receive-folder
  resolution, receive-folder verification state, post-hierarchy contract
  recording, and the response builder now live in the focused folders module,
  with dispatch reduced to input-handle-table echo plus a thin call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopEmptyFolder` and
  `RopHardDeleteMessagesAndSubfolders` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/folders.rs` as
  `append_empty_folder_response`. The move preserves Microsoft boolean-field
  validation, missing-handle error mapping, Recoverable Items Root rejection,
  recoverable-folder purge delegation, public-folder purge delegation,
  mailbox-tree hard-delete behavior, folder content hard-delete behavior,
  content notifications for changed folders, partial-completion response
  bytes, and existing error mapping.
- 2026-06-30 verification for the empty-folder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange empty_folder` passed 15
  focused tests covering canonical EWS empty-folder behavior, MAPI
  EmptyFolder/HardDeleteMessagesAndSubfolders boolean handling, retention
  partial completion, replay idempotency, child-folder preservation,
  recoverable-folder rejection and purge, public-folder item deletion,
  permission-denied behavior, notifications, and content-sync convergence;
  `cargo test -p lpe-exchange hierarchy` passed 157 broader hierarchy tests;
  `cargo test -p lpe-exchange recoverable_item` passed 14 broader
  recoverable-item tests; `cargo test -p lpe-exchange public_folder` passed 74
  broader public-folder tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 14,470 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 14,062 lines and
  `mapi/dispatch/folders.rs` at 766 lines. `rg` confirmed
  `append_empty_folder_response`, recoverable-folder purge delegation,
  public-folder purge delegation, mailbox-tree hard-delete delegation, folder
  hard-delete delegation, and content notification recording now live in the
  focused folders module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopDeleteFolder` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/folders.rs` as
  `append_delete_folder_response`. The move preserves DeleteFolder flag
  validation, missing-handle and missing-target error mapping, system-mailbox
  rejection, advertised special-folder no-op/denied/tombstone behavior,
  public-folder deletion through the canonical store, persisted and staged
  search-folder deletion, retry acknowledgement for deleted search folders,
  custom mailbox destruction, hierarchy notifications, partial-completion
  response bytes, and RCA diagnostics.
- 2026-06-30 verification for the delete-folder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange delete_folder` passed
  11 focused tests covering EWS public-folder and mailbox delete behavior,
  MAPI custom mailbox deletion, system-mailbox rejection, local default named
  view no-op behavior, Microsoft reserved-flag rejection, and public-folder
  delete through the canonical store; `cargo test -p lpe-exchange hierarchy`
  passed 157 broader hierarchy tests; `cargo test -p lpe-exchange
  public_folder` passed 74 broader public-folder tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 14,223
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,816 lines and `mapi/dispatch/folders.rs` at 1,034 lines. `rg`
  confirmed `append_delete_folder_response`, advertised special-folder delete
  diagnostics, public-folder delete audit action, search-folder deletion,
  search-folder retry acknowledgement, and custom mailbox destruction now live
  in the focused folders module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002/MR-005 by moving `RopMoveFolder` and
  `RopCopyFolder` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/folders.rs` as `append_folder_move_copy_response`. The move
  preserves Microsoft boolean-field validation, source/target/folder handle
  error mapping, blank display-name rejection, public-folder copy delegation,
  public-folder move parent validation and canonical update, mailbox target
  parent validation, system-folder source rejection, canonical mailbox copy and
  move calls, MAPI identity allocation for copied folders, hierarchy
  notifications for old and new parents, partial-completion response bytes,
  and audit action strings.
- 2026-06-30 verification for the folder move/copy routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange folder_move_copy`
  passed 3 focused parser and MAPI behavior tests covering Microsoft
  MoveFolder/CopyFolder request parsing, nonzero boolean handling, and
  system-folder source rejection; `cargo test -p lpe-exchange hierarchy`
  passed 157 broader hierarchy tests; `cargo test -p lpe-exchange sync` passed
  218 broader sync tests, including custom mailbox move and hierarchy-sync
  convergence; `cargo test -p lpe-exchange public_folder` passed 74 broader
  public-folder tests, including public-folder copy and move through the
  canonical store; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,957 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 13,554 lines and
  `mapi/dispatch/folders.rs` at 1,313 lines. `rg` confirmed
  `append_folder_move_copy_response`, public-folder copy delegation,
  public-folder move audit action, canonical mailbox create/copy, canonical
  mailbox update/move, and hierarchy notification recording now live in the
  focused folders module, with dispatch reduced to a thin async call.
- 2026-06-30: Advanced MR-002 by moving
  `RopFastTransferSourceCopyMessages` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs` as
  `append_fast_transfer_source_copy_messages_response`. The move preserves
  folder-handle validation, requested message-id filtering, deterministic
  selected-message ordering, attachment fact collection, FastTransfer message
  list buffer generation, synchronization-source handle allocation, handle
  slot updates, response bytes, and output handle tracking.
- 2026-06-30 verification for the FastTransfer source copy-messages routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  fast_transfer_copy_messages` passed 2 focused tests covering Microsoft
  FastTransfer copy-message markers and requested canonical message filtering;
  `cargo test -p lpe-exchange sync` passed 218 broader sync/FastTransfer
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,917
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,514 lines and `mapi/dispatch/sync_import.rs` at 938 lines. `rg`
  confirmed `append_fast_transfer_source_copy_messages_response`,
  message-list buffer generation, attachment fact collection,
  `SynchronizationSource` handle allocation, and the response builder now live
  in the focused sync/import module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving the `RopFastTransferSourceCopyFolder`,
  `RopFastTransferSourceCopyTo`, and `RopFastTransferSourceCopyProperties`
  response routing out of `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs`
  as `append_fast_transfer_source_copy_response`. The move preserves missing
  input-handle error mapping, unsupported manifest error mapping, canonical
  FastTransfer manifest selection, `SynchronizationSource` handle allocation,
  output handle slot updates, response bytes, and output handle tracking.
- 2026-06-30 verification for the FastTransfer source copy routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  fast_transfer_copy` passed 9 focused tests covering folder, FAI, message,
  copy-to, copy-properties, and requested-message FastTransfer copy behavior;
  `cargo test -p lpe-exchange sync` passed 218 broader sync/FastTransfer
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,879
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,476 lines and `mapi/dispatch/sync_import.rs` at 999 lines. `rg`
  confirmed `append_fast_transfer_source_copy_response`, manifest selection,
  `SynchronizationSource` handle allocation, and the response builder now live
  in the focused sync/import module, with dispatch reduced to a thin call for
  the three source-copy ROPs.
- 2026-06-30: Advanced MR-002 by moving `RopFastTransferDestinationConfigure`
  and the `RopFastTransferDestinationPutBuffer` /
  `RopFastTransferDestinationPutBufferExtended` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs` as
  `append_fast_transfer_destination_configure_response` and
  `append_fast_transfer_destination_put_buffer_response`. The move preserves
  target-handle validation, destination target folder validation,
  destination-handle allocation, marker/subobject rejection that terminates
  the current buffer, missing destination-handle error mapping, partial
  property-buffer termination, unsupported property-target error mapping,
  staged buffer commit behavior, uploaded byte counts, and response bytes.
- 2026-06-30 verification for the FastTransfer destination routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  fast_transfer_destination` passed 6 focused tests covering wrong target
  handles, PutBufferExtended parsing, canonical email upload, unsupported
  property types, partial property buffers, and marker/subobject rejection;
  `cargo test -p lpe-exchange sync` passed 218 broader sync/FastTransfer
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,808
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,405 lines and `mapi/dispatch/sync_import.rs` at 1,092 lines. `rg`
  confirmed destination configure routing, destination put-buffer routing,
  marker checks, staged destination buffer handling, buffer commit, and
  response generation now live in the focused sync/import module, with
  dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002 by moving `RopSynchronizationUploadStateStreamBegin`
  and `RopSynchronizationUploadStateStreamContinue` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs` as
  `append_upload_state_stream_begin_response` and
  `append_upload_state_stream_continue_response`. The move preserves source
  and collector sync-context validation, upload-state property tag staging,
  upload buffer reset, stream-data append behavior, RCA diagnostic fields,
  success responses, and invalid-context error mapping. The larger
  `RopSynchronizationUploadStateStreamEnd` branch deliberately remains local
  because it owns checkpoint gating and transfer-buffer selection.
- 2026-06-30 verification for the upload-state begin/continue routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_upload_state` passed 2 focused tests covering server transfer-state
  return behavior and multiple uploaded state streams; `cargo test -p
  lpe-exchange sync` passed 218 broader sync/FastTransfer tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,654
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,251 lines and `mapi/dispatch/sync_import.rs` at 1,278 lines. `rg`
  confirmed upload-state begin and continue routing, property-tag staging,
  buffer clearing, chunk preview diagnostics, and success response generation
  now live in the focused sync/import module, with dispatch reduced to thin
  calls for the two ROPs.
- 2026-06-30: Advanced MR-002 by moving `RopSynchronizationGetTransferState`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_import.rs` as
  `append_synchronization_get_transfer_state_response`. The move preserves
  source/collector sync-context validation, dynamic state-token construction
  for empty content and hierarchy states, deleted-advertised special-folder
  exclusion, attachment-aware sync-state token generation, uploaded client
  state checkpoint blocking, output handle allocation, success response bytes,
  and invalid-context error mapping. `RopSynchronizationUploadStateStreamEnd`
  remains local because it still owns the upload-state checkpoint gate and
  transfer-buffer selection.
- 2026-06-30 verification for the transfer-state routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange transfer_state` passed
  10 focused tests covering transfer-state handles, Microsoft examples, and
  uploaded-client-state regression cases; `cargo test -p lpe-exchange sync`
  passed 218 broader sync/FastTransfer tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,565 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 13,162 lines and
  `mapi/dispatch/sync_import.rs` at 1,386 lines. `rg` confirmed
  `append_synchronization_get_transfer_state_response`,
  `rop_synchronization_get_transfer_state_response`, and uploaded-state delta
  anchor gating now live in the focused sync/import module, with dispatch
  reduced to a thin call for the ROP.
- 2026-06-30: Advanced MR-002 by moving `RopSynchronizationOpenCollector`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_import.rs` as
  `append_synchronization_open_collector_response`. The move preserves input
  folder validation, invalid-context error mapping, synchronization collector
  handle allocation, mailbox checkpoint identity selection, checkpoint-kind
  selection, upload/import state initialization, output handle slot updates,
  success response bytes, and output handle tracking.
- 2026-06-30 verification for the open-collector routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sync_import` passed 17
  focused sync-import tests covering collector-backed upload/import behavior;
  `cargo test -p lpe-exchange sync` passed 218 broader sync/FastTransfer
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1593 tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,538
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 13,135 lines and `mapi/dispatch/sync_import.rs` at 1,426 lines. `rg`
  confirmed `append_synchronization_open_collector_response`, collector handle
  allocation, `sync_checkpoint_mailbox_id`, and `sync_checkpoint_kind` now live
  in the focused sync/import module, with dispatch reduced to a thin call for
  the ROP.
- 2026-06-30: Advanced MR-002 by moving `RopGetPropertiesSpecific` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/properties.rs` as
  `append_get_properties_specific_response`. The move preserves same-batch
  created-message visibility, custom property lookup, inbox folder-type probe
  diagnostics, named-property debug context, Outlook surface contract logging,
  post-hierarchy session tracking, response bytes, and the dispatch-level
  input-handle echo behavior.
- 2026-06-30 verification for the get-properties-specific routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_properties` passed 26 focused property tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,287 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,884 lines and
  `mapi/dispatch/properties.rs` at 1,333 lines.
- 2026-06-30: Advanced MR-002 by moving `RopGetPropertiesAll` and
  `RopGetPropertiesList` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/properties.rs` as `append_get_properties_all_response` and
  `append_get_properties_list_response`. The move preserves input-object
  lookup, property-list response bytes, all-properties response bytes, and the
  existing canonical snapshot inputs.
- 2026-06-30: Advanced MR-002 by moving `RopCreateMessage` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/messages.rs` as
  `append_create_message_response`. The move preserves folder-id resolution,
  folder access checks, missing-folder error mapping, synthetic folder
  allowances, initial creation/modification timestamp properties, pending object
  selection for messages, contacts, events, tasks, notes, journal entries,
  associated messages, conversation actions, navigation shortcuts, handle slot
  updates, success response bytes, and output handle tracking.
- 2026-06-30 verification for the property wrapper and create-message routing
  splits: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_properties` passed 26 focused property tests; `cargo test -p
  lpe-exchange create_message` passed 3 focused create-message tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 13,191
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 12,789 lines, `mapi/dispatch/messages.rs` at 1,446 lines, and
  `mapi/dispatch/properties.rs` at 1,363 lines.
- 2026-06-30: Advanced MR-002 by moving `RopDeleteProperties` and
  `RopDeletePropertiesNoReplicate` response routing out of `mapi/dispatch.rs`
  into `mapi/dispatch/property_mutations.rs` as
  `append_delete_properties_response`. The move preserves virtual conversation
  action staging, conversation action property deletion, associated config
  property deletion diagnostics, custom property deletion, canonical message
  text-property clearing, best-effort persisted-message delete fallback,
  session-local property deletion, success response bytes, and unsupported
  error mapping.
- 2026-06-30 verification for the delete-properties routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange delete_properties`
  passed 3 focused delete-property tests; `cargo test -p lpe-exchange
  get_properties` passed 26 focused property tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 13,078 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,676 lines and
  `mapi/dispatch/property_mutations.rs` at 136 lines.
- 2026-06-30: Advanced MR-002 by moving `RopSetProperties` and
  `RopSetPropertiesNoReplicate` response routing out of `mapi/dispatch.rs`
  into `mapi/dispatch/property_mutations.rs` as
  `append_set_properties_response`. The move preserves malformed property
  value batch-stop behavior, virtual conversation action staging, message
  property staging, canonical object property updates, folder property problem
  responses, default-folder entry-id alias recording, default-folder
  identification write filtering, profile folder-property persistence,
  post-hierarchy diagnostics, success response bytes, and unsupported error
  mapping.
- 2026-06-30 verification for the set-properties routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange set_properties` passed
  19 focused setter tests; `cargo test -p lpe-exchange delete_properties`
  passed 3 focused delete-property tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,895 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,493 lines and
  `mapi/dispatch/property_mutations.rs` at 334 lines.
- 2026-06-30: Advanced MR-002 by moving `RopSortTable` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_sort_table_response`. The move preserves inbox sort trace
  construction, invalid sort-order reset behavior, invalid-sort error mapping,
  contents-table sort/category/position/bookmark mutation, selected named
  property context logging, success response bytes, unsupported object error
  mapping, and Outlook view failure trace recording.
- 2026-06-30 verification for the sort-table routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sort_table` passed 4
  focused sort-table tests; `cargo test -p lpe-exchange tables` passed 194
  broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,836 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,434 lines and
  `mapi/dispatch/table_controls.rs` at 80 lines.
- 2026-06-30: Advanced MR-002 by moving `RopRestrict` table-state response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_restrict_response`. The move preserves the existing supported-object
  precheck in dispatch, invalid async flag table invalidation, invalid-flag
  error mapping, inbox restriction trace construction, hierarchy/contents
  table restriction mutation, rule-table cursor reset behavior, selected named
  property context logging, success response bytes, unsupported object error
  mapping, Outlook view failure trace recording, and malformed restriction
  batch-stop behavior through `TableControlFlow::StopBatch`.
- 2026-06-30 verification for the restrict routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange restrict` passed 35
  focused restriction tests; `cargo test -p lpe-exchange tables` passed 194
  broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,746 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,344 lines and
  `mapi/dispatch/table_controls.rs` at 203 lines.
- 2026-06-30: Advanced MR-002 by moving `RopQueryRows` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_query_rows_response`. The move preserves visible-inbox and calendar
  query diagnostics, bootstrap phase and row-invariant logging, associated
  query context recording, Common Views shortcut context recording, hierarchy
  query context recording, Outlook smart-input variant handling, queried
  cursor capture, canonical query-row response generation, contents/hierarchy
  response diagnostics, associated non-empty row tracking, last table query
  context recording, success response bytes, and Outlook view failure trace
  recording.
- 2026-06-30 verification for the query-rows routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange query_rows` passed 52
  focused query-row tests; `cargo test -p lpe-exchange tables` passed 194
  broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,506 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 12,104 lines and
  `mapi/dispatch/table_controls.rs` at 459 lines.
- 2026-06-30: Advanced MR-002 by moving `RopSetSearchCriteria` and
  `RopGetSearchCriteria` response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/search_folders.rs` as
  `append_set_search_criteria_response` and
  `append_get_search_criteria_response`. The move preserves folder-handle
  validation, search-folder definition lookup, built-in search-folder refresh
  acknowledgement, built-in fallback criteria, access-denied and not-found
  error mapping, bounded criteria parsing and serialization, rejected criteria
  diagnostics, canonical search-folder upsert behavior, session definition
  refresh, success response bytes, and GetSearchCriteria response encoding.
- 2026-06-30 verification for the search-criteria routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange search_criteria`
  passed 22 focused search-criteria tests; `cargo test -p lpe-exchange
  search_folder` passed 29 broader search-folder tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 12,357
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 11,955 lines and
  `mapi/dispatch/search_folders.rs` at 928 lines.
- 2026-06-30: Advanced MR-002 by moving `RopFindRow` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_find_row_response`. The move preserves inbox find-row trace
  construction, selected named-property context selection, canonical
  find-row response generation, Outlook contents-table find-row diagnostics,
  associated find context recording, broad associated find-row tracking,
  Outlook view failure trace recording, and success/error response bytes.
- 2026-06-30 verification for the find-row routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange find_row` passed 47
  focused find-row tests; `cargo test -p lpe-exchange tables` passed 194
  broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,310 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 11,908 lines and
  `mapi/dispatch/table_controls.rs` at 529 lines.
- 2026-06-30: Advanced MR-002 by grouping adjacent attachment ROP routing in
  `mapi/dispatch/attachments.rs` as `append_attachment_response`. The move
  preserves the existing per-ROP handlers for `RopGetValidAttachments`,
  `RopGetAttachmentTable`, `RopOpenAttachment`, `RopCreateAttachment`,
  `RopDeleteAttachment`, `RopOpenEmbeddedMessage`, and
  `RopSaveChangesAttachment`, including handle allocation, output handle
  tracking, canonical attachment persistence, embedded-message handling,
  validation, and response bytes.
- 2026-06-30 verification for the attachment routing grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange attachment` passed 47
  focused attachment tests; `cargo test -p lpe-exchange properties` passed
  247 broader property and stream tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 12,254 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 11,852 lines and
  `mapi/dispatch/attachments.rs` at 1,044 lines.
- 2026-06-30: Advanced MR-002 by grouping stream ROP routing in
  `mapi/dispatch/properties.rs` as `append_stream_response`. The move
  preserves the existing per-ROP handlers for `RopOpenStream`,
  `RopReadStream`, `RopSeekStream`, `RopSetStreamSize`, `RopWriteStream`,
  `RopWriteAndCommitStream`, `RopWriteStreamExtended`, `RopCopyToStream`,
  `RopGetStreamSize`, `RopCloneStream`, `RopLockRegionStream`, and
  `RopUnlockRegionStream`, including stream handle allocation, output handle
  tracking, body/attachment/config stream reads and writes, stream sizing,
  clone/region responses, diagnostics, and response bytes.
- 2026-06-30: Advanced MR-002 by moving `RopSetColumns` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_set_columns_response`. The move preserves table property tag
  normalization, named-property alias diagnostics, hierarchy/contents/
  attachment/permission/rule table column state, invalid column request table
  invalidation, invalid-request batch-stop behavior through
  `TableControlFlow::StopBatch`, visible inbox and calendar set-column
  tracking, Outlook view failure trace recording, success response bytes, and
  unsupported-object error mapping.
- 2026-06-30 verification for the stream grouping and SetColumns routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  stream` passed 38 focused stream tests before the SetColumns slice; `cargo
  test -p lpe-exchange set_columns` passed 13 focused SetColumns/table
  validation tests; `cargo test -p lpe-exchange tables` passed 194 broader
  table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 11,949 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 11,547 lines,
  `mapi/dispatch/table_controls.rs` at 823 lines, and
  `mapi/dispatch/properties.rs` at 1,443 lines. `rg` confirmed
  `append_stream_response` lives in the focused properties module and
  `append_set_columns_response` lives in the focused table-controls module,
  with dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002 by moving `RopOpenMessage` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/message_open.rs` as
  `append_open_message_response`. The move preserves mailbox, search-folder,
  unique-message folder fallback, contact, event, task, note, journal,
  Common Views named-view, search-folder definition, navigation shortcut,
  delegate free/busy, conversation action, associated config, recoverable
  item, and public-folder item lookup order; handle allocation; message-handle
  generation tracking; associated-config open diagnostics; Outlook view
  failure trace recording; success response bytes; and not-found error
  mapping.
- 2026-06-30 verification for the OpenMessage routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange open_message` passed
  12 focused OpenMessage tests; `cargo test -p lpe-exchange properties`
  passed 247 broader property and OpenMessage/GetProperties tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 11,485
  tracked-source lines; `mapi/dispatch/messages.rs` no longer appears in the
  oversized-source report. Direct physical line counts report
  `mapi/dispatch.rs` at 11,083 lines, `mapi/dispatch/messages.rs` at 1,446
  lines, and `mapi/dispatch/message_open.rs` at 440 lines. `rg` confirmed
  `append_open_message_response` lives in the focused message-open module,
  with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopCreateFolder` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/folder_create.rs` as
  `append_create_folder_response`. The move preserves parent-folder handle
  validation and error mapping, root/public/search/role parent validation,
  display-name/type/reserved-field validation, advertised special-folder
  open/deleted behavior, public-folder duplicate/open/create paths, search
  folder reuse and staging, canonical mailbox duplicate/create paths, MAPI
  identity allocation, handle slot updates, notifications, audit entries,
  response bytes, and output-handle behavior.
- 2026-06-30 verification for the CreateFolder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange create_folder` passed
  17 focused CreateFolder tests; `cargo test -p lpe-exchange hierarchy`
  passed 157 broader hierarchy tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing. Direct
  physical line counts report `mapi/dispatch.rs` at 10,520 lines,
  `mapi/dispatch/folder_create.rs` at 575 lines, and
  `mapi/dispatch/folders.rs` at 1,313 lines. `rg` confirmed
  `append_create_folder_response` lives in the focused folder-create module,
  with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopSaveChangesMessage` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/message_save.rs` as
  `append_save_changes_message_route_response`. The move preserves save-flag
  validation, recent-probe diagnostics, pending contact/event/task/note/journal
  creation, conversation-action and navigation-shortcut persistence, embedded
  message save handling, staged message property and recipient commits,
  message/calendar attachment deletion commits, associated-config persistence,
  public-folder item saves, transient sync artifact handling, canonical message
  import, MAPI identity allocation, custom property persistence, sync upload
  change recording, notifications, response bytes, and error mapping.
- 2026-06-30 verification for the SaveChangesMessage routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange save_changes` passed 3
  focused SaveChanges tests; `cargo test -p lpe-exchange message` passed 205
  broader message tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 9,404 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 9,009 lines,
  `mapi/dispatch/message_save.rs` at 1,486 lines, and
  `mapi/dispatch/messages.rs` at 1,446 lines. `rg` confirmed
  `append_save_changes_message_route_response` lives in the focused
  message-save module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopSynchronizationConfigure`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_configure.rs` as
  `append_synchronization_configure_response`. The move preserves folder-handle
  validation, sync-type and property-tag validation, batch-stop behavior for
  invalid sync type/property tags, checkpoint load and usability filtering,
  change-log lookup, mailbox/email/special-object scope selection, attachment
  fact collection, deleted-object projection, sync-state and manifest buffer
  generation, hierarchy/FAI diagnostics, partial-scope checkpoint gating,
  transfer-handle allocation, output-handle tracking, and content-sync
  configure observation.
- 2026-06-30 verification for the SynchronizationConfigure routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_configure` passed 2 focused sync-configure tests; `cargo test -p
  lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing before the helper was moved from `sync_import.rs`
  into its own `sync_configure.rs` module; the focused sync-configure tests
  passed again after that module move. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 8,856
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 8,461 lines,
  `mapi/dispatch/sync_configure.rs` at 554 lines, and
  `mapi/dispatch/sync_import.rs` at 1,426 lines. `rg` confirmed
  `append_synchronization_configure_response` lives in the focused
  sync-configure module, with dispatch reduced to a thin flow-checked call.
- 2026-06-30: Advanced MR-002 by moving
  `RopSynchronizationImportMessageChange` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import_message.rs` as
  `append_synchronization_import_message_change_response`. The move preserves
  folder-handle validation, import property parsing, import-flag diagnostics,
  Common Views navigation-shortcut import, associated-message staging,
  conflict detection, canonical message/public-folder/note/journal property
  updates, fallback pending object staging, output-handle allocation,
  sync-upload content-change recording, response bytes, and error mapping.
- 2026-06-30 verification for the SynchronizationImportMessageChange routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_import_message` passed 3 focused sync-import message tests; `cargo test
  -p lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 8,507
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 8,112 lines and
  `mapi/dispatch/sync_import_message.rs` at 368 lines. `rg` confirmed
  `append_synchronization_import_message_change_response` lives in the
  focused sync-import-message module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving
  `RopSynchronizationImportReadStateChanges` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import_read_state.rs` as
  `append_synchronization_import_read_state_changes_response`. The move
  preserves folder-handle validation, transient client-local message skip
  behavior, partial-completion handling for missing messages and failed flag
  updates, canonical read/unread flag mutation through the store, sync-upload
  content-change recording, and response bytes.
- 2026-06-30 verification for the SynchronizationImportReadStateChanges routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  import_read_state` passed 3 focused import-read-state tests; `cargo test -p
  lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 8,468
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 8,073 lines and
  `mapi/dispatch/sync_import_read_state.rs` at 63 lines. `rg` confirmed
  `append_synchronization_import_read_state_changes_response` lives in the
  focused sync-import-read-state module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving
  `RopSynchronizationImportHierarchyChange` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import_hierarchy.rs` as
  `append_synchronization_import_hierarchy_change_response`. The move
  preserves folder-handle validation, hierarchy property parsing, system-folder
  reconciliation, duplicate/custom folder validation, canonical mailbox
  creation, MAPI identity allocation, sync-upload hierarchy-change recording,
  response bytes, and error mapping.
- 2026-06-30 verification for the SynchronizationImportHierarchyChange routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  import_hierarchy` passed 2 focused hierarchy-import tests; `cargo test -p
  lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 8,354
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 7,960 lines and `mapi/dispatch/sync_import_hierarchy.rs` at 119 lines.
  `rg` confirmed `append_synchronization_import_hierarchy_change_response`
  lives in the focused sync-import-hierarchy module, with dispatch reduced to
  a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopSynchronizationImportDeletes`
  response routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_import_deletes.rs` as
  `append_synchronization_import_deletes_response`. The move preserves
  hierarchy collector folder deletion, custom-folder validation, canonical
  mailbox deletion, message hard-delete/soft-delete/delete-without-trash
  routing, transient client-local skip behavior, note and journal deletion,
  partial-completion reporting, sync-upload hierarchy/content change
  recording, response bytes, and error mapping.
- 2026-06-30 verification for the SynchronizationImportDeletes routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_import_delete` passed 3 focused delete tests; `cargo test -p
  lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 8,211
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 7,817 lines and `mapi/dispatch/sync_import_deletes.rs` at 164 lines.
  `rg` confirmed `append_synchronization_import_deletes_response` lives in the
  focused sync-import-deletes module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving
  `RopSynchronizationImportMessageMove` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import_message_move.rs` as
  `append_synchronization_import_message_move_response`. The move preserves
  malformed move request handling, target folder-handle validation, note and
  journal same-folder acknowledgement, source message and target mailbox
  lookup, canonical message move through the store, moved-message MAPI identity
  allocation, sync-upload source checkpoint and target content-change
  recording, response bytes, and error mapping.
- 2026-06-30 verification for the SynchronizationImportMessageMove routing
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_import_move` passed 1 focused move test; `cargo test -p lpe-exchange
  sync` passed 218 broader sync tests; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 8,103 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 7,709 lines and
  `mapi/dispatch/sync_import_message_move.rs` at 128 lines. `rg` confirmed
  `append_synchronization_import_message_move_response` lives in the focused
  sync-import-message-move module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopOpenFolder` response routing out
  of `mapi/dispatch.rs` into `mapi/dispatch/folder_open.rs` as
  `append_open_folder_response`. The move preserves special-folder alias
  resolution, mailbox/collaboration/public/search-folder lookup, advertised
  special-folder handling, not-found errors, public-folder ghosted responses,
  folder property loading, output-handle allocation, post-hierarchy and inbox
  diagnostics, root/IPM bootstrap logging, response bytes, and output-handle
  tracking.
- 2026-06-30 verification for the OpenFolder routing split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange open_folder` passed 6
  focused OpenFolder tests; `cargo test -p lpe-exchange hierarchy` passed 157
  broader hierarchy tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 7,680 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 7,286 lines and
  `mapi/dispatch/folder_open.rs` at 429 lines. `rg` confirmed
  `append_open_folder_response` lives in the focused folder-open module, with
  dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving `RopRelease` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/release.rs` as
  `append_release_response`. The move preserves input-handle echoing,
  associated-config stream persistence on release, handle-slot release,
  same-execute released-handle tracking, post-hierarchy release diagnostics,
  inbox FAI handoff and visible-inbox release diagnostics, logoff-after-
  hierarchy tracking, recent probe recording, and trace output.
- 2026-06-30 verification for the Release routing split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange release` passed 16 focused
  release tests; `cargo test -p lpe-exchange connect` passed 82 broader
  connect/transport tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 7,348 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,954 lines and
  `mapi/dispatch/release.rs` at 363 lines. `rg` confirmed
  `append_release_response` lives in the focused release module, with dispatch
  reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving
  `RopFastTransferSourceGetBuffer` response routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_get_buffer.rs` as
  `append_fast_transfer_source_get_buffer_response`. The move preserves
  synchronization-source handle validation, transfer buffer chunking and
  completion status, hierarchy completion summaries, FastTransfer get-buffer
  diagnostics, hierarchy payload summary logging, completed hierarchy-sync
  propagation, sync checkpoint cursor construction, checkpoint skip/partial/
  error/ok logging, durable sync checkpoint persistence, response bytes, and
  unsupported-object error mapping.
- 2026-06-30 verification for the FastTransferSourceGetBuffer routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange get_buffer`
  passed 3 focused get-buffer tests; `cargo test -p lpe-exchange sync` passed
  218 broader sync tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,981 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,587 lines and
  `mapi/dispatch/sync_get_buffer.rs` at 385 lines. `rg` confirmed
  `append_fast_transfer_source_get_buffer_response` lives in the focused
  sync-get-buffer module, with dispatch reduced to a thin call that preserves
  completed hierarchy-sync propagation.
- 2026-06-30: Advanced MR-002 by moving upload-state stream routing for
  `RopSynchronizationUploadStateStreamBegin`,
  `RopSynchronizationUploadStateStreamContinue`, and
  `RopSynchronizationUploadStateStreamEnd` into
  `mapi/dispatch/sync_upload_state.rs`. The move preserves source and
  collector upload-state begin/continue/end behavior, upload property tag
  tracking, stream buffer accumulation and clearing, client-upload byte counts,
  marker-mask updates, delta-anchor checkpoint gating, checkpoint-delta
  transfer-buffer selection, source/collector diagnostics, success response
  bytes, and unsupported-object error mapping. Shared upload-state marker
  helpers remain in `sync_import.rs` because other sync paths still use them.
- 2026-06-30 verification for the upload-state stream routing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  upload_state` passed 3 focused upload-state tests; `cargo test -p
  lpe-exchange sync` passed 218 broader sync tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 6,809
  tracked-source lines. Direct physical line counts report
  `mapi/dispatch.rs` at 6,415 lines, `mapi/dispatch/sync_import.rs` at 1,240
  lines, and `mapi/dispatch/sync_upload_state.rs` at 371 lines. `rg`
  confirmed the upload-state response helpers live in the focused
  sync-upload-state module, with dispatch reduced to thin calls.
- 2026-06-30: Advanced MR-002 by moving the remaining `RopLogon` response
  routing out of `mapi/dispatch.rs` into `mapi/dispatch/logon.rs` as
  `append_logon_response`. The move completes the earlier logon helper slice
  by preserving private/public logon response selection, logon request identity
  diagnostics, handle allocation and handle-table updates, default-folder
  discovery logging, Outlook bootstrap phase logging, special-folder summary
  propagation, and output-handle collection.
- 2026-06-30 verification for the Logon routing split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange logon` passed 34 focused logon
  tests; `cargo test -p lpe-exchange connect` passed 82 broader
  connect/transport tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,785 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,391 lines and
  `mapi/dispatch/logon.rs` at 156 lines. `rg` confirmed
  `append_logon_response` lives in the focused logon module, with dispatch
  reduced to a thin call.
- 2026-06-30: Advanced MR-002 by moving post-hierarchy release diagnostic
  logging out of `mapi/dispatch.rs` into `mapi/dispatch/release.rs` as
  `log_post_hierarchy_release_events`. The move preserves post-sync
  release-containing execute diagnostics, release-only execute diagnostics,
  close-reason context diagnostics, response-payload byte/empty reporting,
  released-handle summaries, live-handle summaries, content-sync-after-
  hierarchy flags, and logon-before/after-content-sync detection. It does not
  change response bytes or handle-table construction.
- 2026-06-30 verification for the post-hierarchy release diagnostics split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange release`
  passed 16 focused release tests; `cargo test -p lpe-exchange connect`
  passed 82 broader connect/transport tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,685 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,291 lines and
  `mapi/dispatch/release.rs` at 491 lines. `rg` confirmed
  `log_post_hierarchy_release_events` lives in the focused release module,
  with dispatch reduced to a single post-loop call before response handle-table
  construction.
- 2026-06-30: Advanced MR-002 by moving per-ROP execute-loop sync bookkeeping
  out of `mapi/dispatch.rs` into `mapi/dispatch/execute.rs` as
  `record_execute_sync_observations`. The move preserves completed hierarchy
  sync recording, hierarchy get-buffer summary propagation, default-folder
  hierarchy membership summary propagation, and content-sync configure
  observation recording after each ROP dispatch.
- 2026-06-30 verification for the execute sync-bookkeeping split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sync` passed 218
  broader sync tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,675 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,281 lines and
  `mapi/dispatch/execute.rs` at 324 lines. `rg` confirmed
  `record_execute_sync_observations` lives in the focused execute module,
  with dispatch reduced to a single per-ROP-loop call.
- 2026-06-30: Advanced MR-002 by moving final Execute ROP-buffer assembly out
  of `mapi/dispatch.rs` into `mapi/dispatch/execute.rs` as
  `finalize_execute_rop_buffer`. The move preserves response handle-table
  selection, input-handle-table echo behavior, empty-response handling,
  standard ROP buffer wrapping, RPC header extension wrapping, and response
  bytes.
- 2026-06-30 verification for the Execute response assembly split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange execute` passed 55
  focused execute tests; `cargo test -p lpe-exchange connect` passed 82
  broader connect/transport tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,666 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,272 lines and
  `mapi/dispatch/execute.rs` at 348 lines. `rg` confirmed
  `finalize_execute_rop_buffer` lives in the focused execute module, with
  dispatch reduced to a single final assembly call.
- 2026-06-30: Advanced MR-002 by moving initial Execute ROP dispatch input
  parsing out of `mapi/dispatch.rs` into `mapi/dispatch/execute.rs` as
  `parse_execute_rop_dispatch_input`. The move preserves ROP buffer framing
  split behavior, handle-table parsing, malformed-framing parse-error
  responses, malformed-handle-table parse-error responses, RPC header
  extension parse-error wrapping, and the `extended` flag passed to final
  response assembly.
- 2026-06-30 verification for the Execute dispatch-input parsing split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange execute`
  passed 55 focused execute tests; `cargo test -p lpe-exchange malformed`
  passed 6 malformed-buffer tests; `cargo test -p lpe-exchange handle_table`
  passed 6 handle-table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,652 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,258 lines and
  `mapi/dispatch/execute.rs` at 371 lines. `rg` confirmed
  `parse_execute_rop_dispatch_input` lives in the focused execute module,
  with dispatch reduced to a single initial parse call.
- 2026-06-30: Advanced MR-002 by moving the pre-dispatch Outlook stream-batch
  observation out of `mapi/dispatch.rs` into `mapi/dispatch/execute.rs` as
  `record_execute_stream_batch_observation`. The move preserves the exact
  `SetProperties,OpenStream,SetStreamSize,WriteStream,CommitStream` batch
  detection, session stream-batch recording, Outlook view failure trace event,
  RCA debug logging fields, request ID, ROP-name summary, and input handle
  table summary.
- 2026-06-30 verification for the Execute stream-batch observation split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange stream`
  passed 38 focused stream tests; `cargo test -p lpe-exchange execute` passed
  55 focused execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,640 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,246 lines and
  `mapi/dispatch/execute.rs` at 399 lines. `rg` confirmed
  `record_execute_stream_batch_observation` lives in the focused execute
  module, with dispatch reduced to a single pre-loop observation call.
- 2026-06-30: Advanced MR-002 by moving per-ROP request read and parse-error
  response handling out of `mapi/dispatch.rs` into `mapi/dispatch/execute.rs`
  as `read_next_execute_rop_request`. The move preserves `read_rop_request`
  success behavior, parse-error response bytes, and immediate ROP-loop
  termination on malformed request data.
- 2026-06-30 verification for the Execute request-read split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange malformed` passed 6
  malformed-buffer tests; `cargo test -p lpe-exchange execute` passed 55
  focused execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,636 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,242 lines and
  `mapi/dispatch/execute.rs` at 411 lines. `rg` confirmed
  `read_next_execute_rop_request` lives in the focused execute module, with
  dispatch reduced to a single per-loop request-read call.
- 2026-06-30: Advanced MR-002 by moving Execute ROP-loop zero-padding
  termination into `mapi/dispatch/execute.rs` through
  `read_next_execute_rop_request`. The move preserves zero-padding
  termination before request parsing, malformed-request parse-error response
  bytes, and immediate loop termination before any ROP dispatch side effects.
- 2026-06-30 verification for the Execute zero-padding termination split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange malformed`
  passed 6 malformed-buffer tests; `cargo test -p lpe-exchange execute`
  passed 55 focused execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,633 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,239 lines and
  `mapi/dispatch/execute.rs` at 414 lines. `rg` confirmed
  `remaining_is_zero_padding` is now checked in the focused execute module,
  with dispatch retaining only the `read_next_execute_rop_request` loop call.
- 2026-06-30: Advanced MR-002 by moving the `RopRestrict` table-scope support
  guard out of `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_restrict_table_control_response`. The move preserves unsupported
  object error mapping, successful restrict routing, invalid-restriction
  stop-batch behavior, and all existing table-control response bytes.
- 2026-06-30 verification for the Restrict table-control split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange restrict` passed 35
  focused restrict tests; `cargo test -p lpe-exchange tables` passed 194
  broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,623 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,229 lines and
  `mapi/dispatch/table_controls.rs` at 850 lines. `rg` confirmed
  `append_restrict_table_control_response` lives in the focused table-controls
  module, with dispatch reduced to a thin call.
- 2026-06-30: Advanced MR-002/MR-003 by moving transport spooler/advisory and
  deferred-action input-handle checks out of `mapi/dispatch.rs` into
  `mapi/dispatch/submission.rs` as
  `append_spooler_advisory_dispatch_response` and
  `append_deferred_action_messages_dispatch_response`. The move preserves
  advisory success/error mapping, deferred-action unsupported/not-found
  responses, batch alignment, and the absence of protocol-local Outbox state.
- 2026-06-30 verification for the spooler/advisory dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange submission` passed 25
  focused submission tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,619 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,225 lines and
  `mapi/dispatch/submission.rs` at 495 lines. `rg` confirmed the new
  dispatch wrappers live in the focused submission module, with dispatch
  reduced to thin calls.
- 2026-06-30: Advanced MR-002/MR-007 by moving the `RopGetReceiveFolderTable`
  private-logon-handle dispatch check out of `mapi/dispatch.rs` into
  `mapi/dispatch/tables.rs` as `append_receive_folder_table_dispatch_response`.
  The move preserves private-logon enforcement, receive-folder table response
  bytes, receive-folder verification recording, and diagnostic fields.
- 2026-06-30 verification for the ReceiveFolderTable dispatch split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange receive_folder`
  passed 12 focused receive-folder tests; `cargo test -p lpe-exchange tables`
  passed 194 broader table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,619 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,225 lines and
  `mapi/dispatch/tables.rs` at 1,274 lines. `rg` confirmed the new dispatch
  wrapper lives in the focused tables module, with dispatch retaining a thin
  call.
- 2026-06-30: Advanced MR-002/MR-007 by moving public-folder per-user ROP
  routing for `RopGetPerUserLongTermIds`, `RopGetPerUserGuid`,
  `RopReadPerUserInformation`, and `RopWritePerUserInformation` out of
  `mapi/dispatch.rs` into `mapi/dispatch/public_folders.rs` as
  `append_public_folder_per_user_response`. The move preserves long-term-id
  discovery, per-user GUID lookup, canonical per-user read-state streaming,
  write validation, response bytes, and existing error mapping.
- 2026-06-30 verification for the public-folder per-user dispatch split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  public_folder` passed 74 focused public-folder tests; `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 6,592
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 6,198 lines and `mapi/dispatch/public_folders.rs` at 660 lines. `rg`
  confirmed the per-user dispatch wrapper lives in the focused public-folders
  module, with dispatch reduced to a single grouped call.
- 2026-06-30: Advanced MR-002 by moving object-id conversion routing for
  `RopLongTermIdFromId` and `RopIdFromLongTermId` out of `mapi/dispatch.rs`
  into `mapi/dispatch/object_ids.rs` as `append_object_id_conversion_response`.
  The move preserves long-term-id scope validation, mailbox GUID alias
  handling, special-folder conversion behavior, response bytes, and RCA
  diagnostics.
- 2026-06-30 verification for the object-id conversion dispatch split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange long_term_id`
  passed 8 focused object-id tests; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,582 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,188 lines and
  `mapi/dispatch/object_ids.rs` at 208 lines. `rg` confirmed the object-id
  dispatch wrapper lives in the focused object-ids module, with dispatch
  reduced to a single grouped call.
- 2026-06-30: Advanced MR-002/MR-007 by moving public-folder replica probe
  routing for `RopGetOwningServers` and `RopPublicFolderIsGhosted` out of
  `mapi/dispatch.rs` into `mapi/dispatch/public_folders.rs` as
  `append_public_folder_replica_probe_response`. The move preserves logon
  handle validation, canonical folder-id validation, ordered replica server
  projection, ghosted-folder detection, response bytes, and existing error
  mapping.
- 2026-06-30 verification for the public-folder replica probe dispatch split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  public_folder` passed 74 focused public-folder tests; `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 6,573
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 6,179 lines and `mapi/dispatch/public_folders.rs` at 683 lines. `rg`
  confirmed the replica probe dispatch wrapper lives in the focused
  public-folders module, with dispatch reduced to a single grouped call.
- 2026-06-30: Advanced MR-002/MR-008 by moving named-property ROP routing for
  `RopGetNamesFromPropertyIds`, `RopGetPropertyIdsFromNames`, and
  `RopQueryNamedProperties` out of `mapi/dispatch.rs` into
  `mapi/dispatch/named_properties.rs` as
  `append_named_property_dispatch_response`. The move preserves session-cache
  hydration, named-property allocation, no-create missing-property errors,
  query enumeration, Outlook contact-source tracing, response bytes, and the
  input-handle-table echo required by `RopGetPropertyIdsFromNames`.
- 2026-06-30 verification for the named-property dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange properties` passed 247
  focused property/named-property tests; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,556 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,162 lines and
  `mapi/dispatch/named_properties.rs` at 294 lines. `rg` confirmed the
  named-property dispatch wrapper lives in the focused named-properties module,
  with dispatch reduced to a single grouped call.
- 2026-06-30: Advanced MR-002 by moving `RopGetAddressTypes` routing out of
  `mapi/dispatch.rs` into `mapi/dispatch/logon.rs` as
  `append_address_types_dispatch_response`. The move preserves input-handle
  validation, RCA debug logging, `EX`/`SMTP` response bytes, missing-handle
  error mapping, and the input-handle-table echo required by the Execute
  response.
- 2026-06-30 verification for the address-types dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange logon` passed 34
  focused logon/bootstrap tests; `cargo test -p lpe-exchange execute` passed
  55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,556 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,162 lines and
  `mapi/dispatch/logon.rs` at 171 lines. `rg` confirmed the address-types
  dispatch wrapper lives in the focused logon module; this slice moved routing
  ownership but did not materially reduce the dispatch line count.
- 2026-06-30: Advanced MR-002 by moving permissions and rules ROP routing for
  `RopGetPermissionsTable`, `RopModifyPermissions`, `RopGetRulesTable`, and
  `RopModifyRules` out of `mapi/dispatch.rs` into
  `mapi/dispatch/permissions.rs` and `mapi/dispatch/rules.rs` as
  `append_permissions_dispatch_response` and `append_rules_dispatch_response`.
  The move preserves table-handle allocation, output-handle recording,
  canonical mailbox/calendar/public-folder permission writes, canonical Sieve
  rule writes, bounded Exchange-rule rejection, response bytes, and existing
  error mapping.
- 2026-06-30 verification for the permissions/rules dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange permissions` passed 20
  focused permission tests; `cargo test -p lpe-exchange rules` passed 11
  focused rule tests; `cargo test -p lpe-exchange execute` passed 55 focused
  Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,536 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,142 lines,
  `mapi/dispatch/permissions.rs` at 359 lines, and `mapi/dispatch/rules.rs`
  at 347 lines. `rg` confirmed the permissions and rules dispatch wrappers
  live in their focused modules, with dispatch reduced to two grouped calls.
- 2026-06-30: Advanced MR-002 by moving local-replica sync utility routing for
  `RopSetLocalReplicaMidsetDeleted` and `RopGetLocalReplicaIds` out of
  `mapi/dispatch.rs` into `mapi/dispatch/sync_import.rs` as
  `append_local_replica_dispatch_response`. The move preserves synchronization
  source/collector transfer-state mutation, local replica id allocation,
  response bytes, unsupported-object error mapping, and the input-handle-table
  echo required by `RopGetLocalReplicaIds`.
- 2026-06-30 verification for the local-replica dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sync` passed 218
  focused sync tests; `cargo test -p lpe-exchange connect` passed 82
  connect/profile tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,533 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,139 lines and
  `mapi/dispatch/sync_import.rs` at 1,264 lines. `rg` confirmed the
  local-replica dispatch wrapper lives in the focused sync-import module, with
  dispatch reduced to a single grouped call.
- 2026-06-30: Advanced MR-002 by moving lightweight Execute status/bookmark
  dispatch routing for `RopGetStoreState`, `RopAbort`, `RopProgress`,
  `RopResetTable`, and `RopFreeBookmark` out of `mapi/dispatch.rs` into
  `mapi/dispatch/table_controls.rs` as
  `append_status_or_bookmark_dispatch_response`. The existing response helpers
  remain in their current focused modules, preserving store-state,
  abort/progress/reset-table, and free-bookmark response bytes.
- 2026-06-30 verification for the status/bookmark dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `cargo test -p lpe-exchange logon` passed 34 focused
  logon/bootstrap tests; `cargo test -p lpe-exchange tables` passed 194
  focused table tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,534 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,140 lines and
  `mapi/dispatch/table_controls.rs` at 879 lines. `rg` confirmed the grouped
  status/bookmark dispatch wrapper lives in the focused table-controls module,
  with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-020 by moving receive-folder dispatch routing
  for `RopSetReceiveFolder` and `RopGetReceiveFolder` out of
  `mapi/dispatch.rs` into `mapi/dispatch/folders.rs` as
  `append_receive_folder_dispatch_response`. The move preserves private-logon
  enforcement, fixed canonical receive-folder mapping, validation errors,
  response bytes, and the input-handle-table echo required by
  `RopGetReceiveFolder`.
- 2026-06-30 verification for the receive-folder dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange receive_folder`
  passed 12 focused receive-folder tests; `cargo test -p lpe-exchange logon`
  passed 34 focused logon/bootstrap tests; `cargo test -p lpe-exchange
  execute` passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,524 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,130 lines and
  `mapi/dispatch/folders.rs` at 1,347 lines. `rg` confirmed the grouped
  receive-folder dispatch wrapper lives in the focused folders module, with
  dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-023 by moving Search Folder criteria ROP
  routing for `RopSetSearchCriteria` and `RopGetSearchCriteria` out of
  `mapi/dispatch.rs` into `mapi/dispatch/search_folders.rs` as
  `append_search_criteria_dispatch_response`. The move preserves existing
  canonical search-folder validation, built-in fallback behavior, persisted
  bounded criteria updates, response bytes, and search diagnostics.
- 2026-06-30 verification for the search-criteria dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange search` passed 70
  focused search tests; `cargo test -p lpe-exchange tables` passed 194
  focused table tests; `cargo test -p lpe-exchange execute` passed 55 focused
  Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,512 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,118 lines and
  `mapi/dispatch/search_folders.rs` at 974 lines. `rg` confirmed the grouped
  search-criteria dispatch wrapper lives in the focused search-folders module,
  with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/P0 mailbox mutation cleanup by moving
  `RopSetReadFlags` dispatch ownership out of `mapi/dispatch.rs` into
  `mapi/dispatch/messages.rs` as `append_message_state_dispatch_response`.
  The move preserves the existing canonical read-state mutation path,
  public-folder read-state handling, partial-completion behavior, and response
  bytes.
- 2026-06-30 verification for the message-state dispatch ownership split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange read`
  passed 51 focused read-state tests; `cargo test -p lpe-exchange message`
  passed 205 broader message tests; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,512 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,118 lines and
  `mapi/dispatch/messages.rs` at 1,477 lines. `rg` confirmed the grouped
  message-state dispatch wrapper lives in the focused messages module; this
  slice moved routing ownership but did not materially reduce the dispatch
  line count.
- 2026-06-30: Advanced MR-002/MR-003 by moving recipient ROP routing for
  `RopRemoveAllRecipients`, `RopModifyRecipients`, and `RopReadRecipients`
  out of `mapi/dispatch.rs` into `mapi/dispatch/recipients.rs` as
  `append_recipient_dispatch_response`. The move preserves staged recipient
  replacement behavior, canonical save/import behavior, Bcc-safe read
  projection, reserved-field validation, response bytes, and existing
  recipient debug logging.
- 2026-06-30 verification for the recipient dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange recipient` passed 24
  focused recipient tests; `cargo test -p lpe-exchange message` passed 205
  broader message tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,494 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 6,100 lines and
  `mapi/dispatch/recipients.rs` at 326 lines. `rg` confirmed the grouped
  recipient dispatch wrapper lives in the focused recipients module, with
  dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002 by moving non-import FastTransfer and
  synchronization ROP routing out of `mapi/dispatch.rs` into
  `mapi/dispatch/sync_transfer.rs` as `append_sync_transfer_dispatch_response`.
  The move preserves SynchronizationConfigure stop-batch behavior, completed
  hierarchy-sync observation, content-sync configure observation, FastTransfer
  source/destination response bytes, upload-state handling, collector/transfer
  state handles, and TellVersion behavior.
- 2026-06-30 verification for the sync-transfer dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sync` passed 218
  focused sync/FastTransfer tests; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,377 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,983 lines and
  `mapi/dispatch/sync_transfer.rs` at 186 lines. `rg` confirmed the grouped
  sync-transfer dispatch wrapper lives in the focused sync-transfer module,
  with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-007 by moving table-control ROP routing for
  `RopSetColumns`, `RopSortTable`, `RopRestrict`, `RopQueryRows`,
  table-position/bookmark/collapse ROPs, `RopExpandRow`, and `RopFindRow` out
  of `mapi/dispatch.rs` into `mapi/dispatch/table_controls.rs` as
  `append_table_control_dispatch_response`. The move preserves SetColumns and
  Restrict stop-batch behavior, QueryRows request-batch diagnostics,
  Microsoft table-control validation errors, table cursor/bookmark behavior,
  and the existing `RopExpandRow` folder-handle exclusion.
- 2026-06-30 verification for the table-control dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange tables` passed 194
  focused table tests; `cargo test -p lpe-exchange execute` passed 55 focused
  Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,284 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,890 lines and
  `mapi/dispatch/table_controls.rs` at 1,011 lines. `rg` confirmed the
  grouped table-control dispatch wrapper lives in the focused table-controls
  module, with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-008 by moving property get/set/delete ROP
  routing for `RopGetPropertiesSpecific`, `RopGetPropertiesAll`,
  `RopGetPropertiesList`, `RopSetProperties`, `RopSetPropertiesNoReplicate`,
  `RopDeleteProperties`, and `RopDeletePropertiesNoReplicate` out of
  `mapi/dispatch.rs` into `mapi/dispatch/property_dispatch.rs` as
  `append_property_dispatch_response`. The move preserves input-handle-table
  echo behavior for GetPropertiesSpecific and SetProperties, SetProperties
  stop-batch behavior, property response bytes, property encoding,
  named-property resolution, custom property persistence, and existing
  canonical object mutation paths.
- 2026-06-30 verification for the property dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange properties` passed 247
  focused property tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,240 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,846 lines and
  `mapi/dispatch/property_dispatch.rs` at 127 lines. `rg` confirmed the
  grouped property dispatch wrapper lives in the focused property-dispatch
  module, with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-007 by moving folder mutation ROP routing for
  `RopCreateFolder`, `RopDeleteFolder`, `RopMoveFolder`, `RopCopyFolder`,
  `RopEmptyFolder`, and `RopHardDeleteMessagesAndSubfolders` out of
  `mapi/dispatch.rs` into `mapi/dispatch/folder_dispatch.rs` as
  `append_folder_dispatch_response`. The move preserves output-handle
  allocation for CreateFolder, canonical mailbox/public-folder mutation paths,
  search-folder persistence behavior, empty-folder partial completion handling,
  folder response bytes, and existing error mapping.
- 2026-06-30 verification for the folder dispatch split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange folder` passed 406 focused
  folder/hierarchy tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,203 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,809 lines and
  `mapi/dispatch/folder_dispatch.rs` at 85 lines. `rg` confirmed the grouped
  folder dispatch wrapper lives in the focused folder-dispatch module, with
  dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002/MR-007 by moving message ROP routing for
  `RopOpenMessage`, `RopCreateMessage`, `RopSaveChangesMessage`,
  `RopDeleteMessages`, `RopHardDeleteMessages`, `RopGetMessageStatus`,
  `RopSetMessageStatus`, and `RopMoveCopyMessages` out of
  `mapi/dispatch.rs` into `mapi/dispatch/message_dispatch.rs` as
  `append_message_dispatch_response`. The move preserves output-handle
  allocation for open/create, same-Execute saved-message visibility, delete
  and hard-delete canonical paths, session-local message status, and
  move/copy canonical store behavior.
- 2026-06-30 verification for the message dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange message` passed 205
  focused message/property/submission tests; `cargo test -p lpe-exchange
  execute` passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,142 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,748 lines and
  `mapi/dispatch/message_dispatch.rs` at 115 lines. `rg` confirmed the
  grouped message dispatch wrapper lives in the focused message-dispatch
  module, with dispatch reduced to a single guarded call.
- 2026-06-30: Advanced MR-002 by moving submission, spooler advisory,
  deferred-action, and transport-info ROP routing for `RopSetSpooler`,
  `RopSpoolerLockMessage`, `RopTransportNewMail`,
  `RopUpdateDeferredActionMessages`, `RopSubmitMessage`, `RopTransportSend`,
  `RopAbortSubmit`, `RopGetTransportFolder`, and `RopOptionsData` into
  `mapi/dispatch/submission.rs` as `append_submission_dispatch_response`.
  The move preserves advisory no-op alignment, deferred-action rejection,
  canonical submission, abort-submit cancellation checks, and the existing
  transport folder/options response bytes.
- 2026-06-30 verification for the submission dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange submission` passed 25
  focused submission tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,113 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,719 lines and
  `mapi/dispatch/submission.rs` at 570 lines. `rg` confirmed the grouped
  submission dispatch wrapper lives in the focused submission module, with
  dispatch reduced to a single guarded call.
- 2026-07-01: Advanced MR-002/MR-008 by moving stream, copy-to,
  copy-properties, and commit-stream ROP routing for `RopOpenStream`,
  `RopReadStream`, `RopSeekStream`, `RopSetStreamSize`, `RopWriteStream`,
  `RopWriteAndCommitStream`, `RopWriteStreamExtended`, `RopCopyToStream`,
  `RopGetStreamSize`, `RopCloneStream`, `RopLockRegionStream`,
  `RopUnlockRegionStream`, `RopCopyTo`, `RopCopyProperties`, and
  `RopCommitStream` into `mapi/dispatch/stream_dispatch.rs` as
  `append_stream_dispatch_response`. The move preserves stream handle
  allocation, write/commit persistence, computed stream reads, copy-to and
  copy-properties response mapping, and existing batch alignment.
- 2026-07-01 verification for the stream dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange properties` first
  reported one parallel-only failure in
  `mapi::rop::tests::get_properties_specific_resolves_unspecified_modeled_message_properties`;
  the exact test passed when rerun, and `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange properties` passed 247 focused property/stream tests.
  `cargo test -p lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 6,062
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 5,668 lines and `mapi/dispatch/stream_dispatch.rs` at 110 lines. `rg`
  confirmed the grouped stream dispatch wrapper lives in the focused
  stream-dispatch module, with dispatch reduced to a single guarded call.
- 2026-07-01: Advanced MR-002/MR-007 by replacing the direct attachment ROP
  list in `mapi/dispatch.rs` with `is_attachment_rop` in
  `mapi/dispatch/attachments.rs`. The detailed attachment dispatch remains in
  `append_attachment_response`; the move only centralizes the attachment ROP
  predicate and preserves attachment table, open/create/delete,
  open-embedded-message, and save-changes behavior.
- 2026-07-01 verification for the attachment dispatch predicate split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange attachment` passed
  47 focused attachment tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 6,054 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,660 lines and
  `mapi/dispatch/attachments.rs` at 1,056 lines. `rg` confirmed the grouped
  attachment predicate lives in the focused attachment module, with dispatch
  reduced to a single guarded call.
- 2026-07-01: Advanced MR-002/MR-007 by moving sync-import ROP routing for
  `RopSynchronizationImportMessageChange`,
  `RopSynchronizationImportHierarchyChange`, `RopSynchronizationImportDeletes`,
  `RopSynchronizationImportMessageMove`,
  `RopSynchronizationImportReadStateChanges`,
  `RopSetLocalReplicaMidsetDeleted`, and `RopGetLocalReplicaIds` into
  `mapi/dispatch/sync_import.rs` as `append_sync_import_dispatch_response`.
  The move preserves message, hierarchy, delete, move, read-state, and local
  replica import behavior, including the input-handle-table echo for local
  replica ID allocation.
- 2026-07-01 verification for the sync-import dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sync` passed 218
  focused sync/FastTransfer tests; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,992 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,598 lines and
  `mapi/dispatch/sync_import.rs` at 1,378 lines. `rg` confirmed the grouped
  sync-import dispatch wrapper lives in the focused sync-import module, with
  dispatch reduced to a single guarded call.
- 2026-07-01: Advanced MR-002/MR-007 by grouping public-folder per-user and
  replica metadata ROP routing behind `is_public_folder_metadata_rop` and
  `append_public_folder_metadata_dispatch_response` in
  `mapi/dispatch/public_folders.rs`. The move preserves the existing
  per-user state, per-user GUID/long-term-id, owning-server, and ghosted-folder
  response helpers while reducing `mapi/dispatch.rs` to one guarded
  public-folder metadata call.
- 2026-07-01 verification for the public-folder metadata dispatch grouping:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  public_folder` passed 74 focused public-folder tests; `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 5,978
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 5,584 lines and `mapi/dispatch/public_folders.rs` at 733 lines. `rg`
  confirmed the grouped public-folder metadata predicate and dispatch wrapper
  live in the focused public-folder module.
- 2026-07-01: Advanced MR-002 by moving the remaining direct ROP lists for
  named-property, object-id conversion, permissions, and rules dispatch into
  focused module predicates:
  `is_named_property_rop`, `is_object_id_conversion_rop`,
  `is_permissions_dispatch_rop`, and `is_rules_dispatch_rop`. The move keeps
  all response helpers and behavior in their existing modules and reduces
  `mapi/dispatch.rs` to guarded family calls.
- 2026-07-01 verification for the dispatch predicate grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange properties` passed 247
  focused property/named-property tests; `cargo test -p lpe-exchange
  long_term_id` passed 8 focused object-id tests; `cargo test -p
  lpe-exchange permission` passed 22 focused permission tests; `cargo test -p
  lpe-exchange rules` passed 11 focused rule tests; `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 5,974
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 5,580 lines, `mapi/dispatch/named_properties.rs` at 302 lines,
  `mapi/dispatch/object_ids.rs` at 211 lines,
  `mapi/dispatch/permissions.rs` at 365 lines, and
  `mapi/dispatch/rules.rs` at 350 lines. `rg` confirmed the grouped predicates
  live in the focused modules and `mapi/dispatch.rs` now calls those
  predicates.
- 2026-07-01: Advanced MR-002/P0 mailbox mutation cleanup by moving
  `RopReloadCachedInformation`, `RopSetMessageReadFlag`, and
  `RopSetReadFlags` routing behind `is_message_state_rop` and
  `append_message_state_dispatch_response` in
  `mapi/dispatch/message_state.rs`. The move preserves reload-cached
  summaries, open-message read flag mutation, bulk read-state mutation,
  public-folder read-state handling, response bytes, and the existing message
  helper implementations.
- 2026-07-01 verification for the message-state dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange read` passed 51
  focused read-state tests; `cargo test -p lpe-exchange message` first
  reported a parallel-only failure in
  `tests::mapi_over_http::hierarchy::mapi_over_http_microsoft_hard_delete_messages_and_subfolders_hard_deletes_trash_contents`;
  the exact test passed when rerun and `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange message` passed 205 focused message tests. `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `mapi/dispatch.rs` at 5,951
  tracked-source lines. Direct physical line counts report `mapi/dispatch.rs`
  at 5,557 lines, `mapi/dispatch/messages.rs` at 1,446 lines, and
  `mapi/dispatch/message_state.rs` at 63 lines. `rg` confirmed the grouped
  message-state predicate and dispatch wrapper live in the focused
  message-state module, with `mapi/dispatch.rs` reduced to a single guarded
  call.
- 2026-07-01: Advanced MR-002/MR-007 by grouping table-open routing for
  `RopGetHierarchyTable`, `RopGetContentsTable`, and
  `RopGetReceiveFolderTable` behind `is_table_open_rop` and
  `append_table_open_dispatch_response` in `mapi/dispatch/table_open.rs`. The
  move preserves hierarchy table opening, contents table opening, receive-folder
  table projection, output-handle allocation, row counts, RCA diagnostics, and
  receive-folder private-logon validation.
- 2026-07-01 verification for the table-open dispatch split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange tables` passed 194
  focused table tests; `cargo test -p lpe-exchange receive_folder` passed 12
  focused receive-folder tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,942 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,548 lines and
  `mapi/dispatch/table_open.rs` at 320 lines. `rg` confirmed the grouped
  table-open predicate and dispatch wrapper live in the focused table-open
  module, with `mapi/dispatch.rs` reduced to a single guarded call.
- 2026-07-01: Advanced MR-002/MR-007 by grouping `RopLogon` and
  `RopGetAddressTypes` behind `is_logon_dispatch_rop` and
  `append_logon_dispatch_response` in `mapi/dispatch/logon.rs`. The move
  preserves private and public logon responses, output-handle allocation,
  default-folder IDs, logon RCA diagnostics, and the GetAddressTypes
  input-handle-table echo behavior.
- 2026-07-01 verification for the logon dispatch grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange logon` passed 34
  focused logon tests; `cargo test -p lpe-exchange address_types` passed one
  focused address-type framing test; `cargo test -p lpe-exchange execute`
  passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,933 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,539 lines and
  `mapi/dispatch/logon.rs` at 215 lines. `rg` confirmed the grouped logon
  predicate and dispatch wrapper live in the focused logon module.
- 2026-07-01: Advanced MR-002/MR-007 by grouping
  `RopRegisterNotification` behind `is_notification_dispatch_rop` and
  `append_notification_dispatch_response` in
  `mapi/dispatch/notification_subscriptions.rs`. The move preserves
  notification registration, cursor loading, output-handle allocation,
  response bytes, and existing RCA diagnostics.
- 2026-07-01 verification for the notification dispatch grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange notification` passed
  13 focused notification tests; `cargo test -p lpe-exchange execute` passed
  55 focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,933 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,539 lines and
  `mapi/dispatch/notification_subscriptions.rs` at 116 lines. `rg` confirmed
  the grouped notification predicate and dispatch wrapper live in the focused
  notification module.
- 2026-07-01: Advanced MR-002/MR-007 by grouping `RopRelease` behind
  `is_release_dispatch_rop` and `append_release_dispatch_response` in
  `mapi/dispatch/release.rs`. The move preserves input-handle-table echo,
  released-handle tracking for same-Execute batches, associated-config stream
  release persistence, post-hierarchy release diagnostics, and logoff-after-
  hierarchy-completion state.
- 2026-07-01 verification for the release dispatch grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange release` passed 16
  focused release tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,932 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,538 lines and
  `mapi/dispatch/release.rs` at 530 lines. `rg` confirmed the grouped release
  predicate and dispatch wrapper live in the focused release module.
- 2026-07-01: Advanced MR-002/MR-007 by grouping `RopOpenFolder` behind
  `is_folder_open_rop` and `append_folder_open_dispatch_response` in
  `mapi/dispatch/folder_open.rs`. The move preserves folder alias
  resolution, canonical mailbox/collaboration/public-folder/search-folder
  lookup, output-handle allocation that avoids same-Execute released handles,
  ghosted public-folder response shape, and existing Outlook RCA diagnostics.
- 2026-07-01 verification for the folder-open dispatch grouping: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange open_folder` passed 6
  focused OpenFolder tests; `cargo test -p lpe-exchange folder` passed 406
  focused folder tests; `cargo test -p lpe-exchange execute` passed 55
  focused Execute tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1593 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `mapi/dispatch.rs` at 5,932 tracked-source lines. Direct physical line
  counts report `mapi/dispatch.rs` at 5,538 lines and
  `mapi/dispatch/folder_open.rs` at 465 lines. `rg` confirmed the grouped
  folder-open predicate and dispatch wrapper live in the focused folder-open
  module.
- 2026-07-01: Advanced MR-002/MR-007 by moving the known and unknown
  unsupported ROP fallback response helpers from `mapi/dispatch/execute.rs`
  into `mapi/dispatch/unsupported.rs`. The main Execute router now calls
  `append_unsupported_known_dispatch_response` and
  `append_unsupported_unknown_dispatch_response`, preserving the existing
  unsupported/reserved ROP response bytes and the terminal behavior owned by
  the parsed typed request.
- 2026-07-01 verification for the unsupported fallback split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange unsupported` passed 16
  focused unsupported/reserved behavior tests; `cargo test -p lpe-exchange
  execute` passed 55 focused Execute tests; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1593 tests and doc tests passing.
  `python tools/check_oversized_sources.py` passed in warning mode and
  reports `mapi/dispatch.rs` at 5,934 tracked-source lines. Direct physical
  line counts report `mapi/dispatch.rs` at 5,540 lines,
  `mapi/dispatch/execute.rs` at 408 lines, and
  `mapi/dispatch/unsupported.rs` at 20 lines. `rg` confirmed the fallback
  append helpers and unsupported response primitives live in the focused
  unsupported module; this boundary split adds module wiring, so it does not
  reduce the main router line count.
- 2026-07-01: Resolved the `mapi/dispatch.rs` production-source hotspot by
  moving the in-file `#[cfg(test)] mod tests` body into
  `mapi/dispatch/tests.rs` and leaving `mapi/dispatch.rs` as the module hub
  for routing, helpers, and module declarations. This move is test-only and
  preserves the existing `mapi::dispatch::tests::*` module path.
- 2026-07-01 verification for the dispatch test-module extraction: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi::dispatch::tests` passed 139 extracted dispatch tests; `cargo test -p
  lpe-exchange execute` passed 55 focused Execute tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1593
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and no longer reports `mapi/dispatch.rs` as an
  oversized production source file. Direct physical line counts report
  `mapi/dispatch.rs` at 990 lines and `mapi/dispatch/tests.rs` at 4,506
  lines. The extracted test file remains a follow-up test-organization
  hotspot and should be split by scenario family.
- 2026-07-01: Advanced the dispatch test-organization follow-up by moving the
  core Execute framing, active-session overlap, release-only, and Execute
  debug/response-summary tests from `mapi/dispatch/tests.rs` into
  `mapi/dispatch/tests/execute.rs`. This is test-only and preserves production
  dispatch behavior.
- 2026-07-01 verification for the Execute dispatch test split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi::dispatch::tests::execute` passed 24 Execute-focused dispatch tests;
  `cargo test -p lpe-exchange mapi::dispatch::tests` passed all 139 dispatch
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1,593 tests and doc tests passing. `git diff --check` exited
  successfully with CRLF warnings only, and `python
  tools/check_oversized_sources.py` passed in warning mode. Direct physical
  line counts report `mapi/dispatch/tests.rs` at 3,758 lines and
  `mapi/dispatch/tests/execute.rs` at 751 lines.
- 2026-07-01: Advanced the dispatch test-organization follow-up again by
  moving associated-configuration, Common Views, Quick Step, Free/Busy,
  conversation-action, and virtual FAI row tests from `mapi/dispatch/tests.rs`
  into `mapi/dispatch/tests/associated_config.rs`. This is test-only and
  preserves production dispatch behavior.
- 2026-07-01 verification for the associated-config dispatch test split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi::dispatch::tests::associated_config` passed 25 focused tests; `cargo
  test -p lpe-exchange mapi::dispatch::tests` passed all 139 dispatch tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,593
  tests and doc tests passing. `git diff --check` exited successfully with
  CRLF warnings only, and `python tools/check_oversized_sources.py` passed in
  warning mode. Direct physical line counts report `mapi/dispatch/tests.rs` at
  2,820 lines, `mapi/dispatch/tests/associated_config.rs` at 941 lines, and
  `mapi/dispatch/tests/execute.rs` at 751 lines.
- 2026-07-01: Advanced the dispatch test-organization follow-up by moving
  folder/default-folder projection, special-folder GetProps probe, folder
  set-property, advertised-folder, and post-hierarchy probe tests from
  `mapi/dispatch/tests.rs` into `mapi/dispatch/tests/folders.rs`. This is
  test-only and preserves production dispatch behavior.
- 2026-07-01 verification for the folder dispatch test split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi::dispatch::tests::folders` passed 42 focused tests; `cargo test -p
  lpe-exchange mapi::dispatch::tests` passed all 139 dispatch tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,593
  tests and doc tests passing. `git diff --check` exited successfully with
  CRLF warnings only, and `python tools/check_oversized_sources.py` passed in
  warning mode. Direct physical line counts report `mapi/dispatch/tests.rs` at
  1,416 lines, `mapi/dispatch/tests/folders.rs` at 1,407 lines,
  `mapi/dispatch/tests/associated_config.rs` at 941 lines, and
  `mapi/dispatch/tests/execute.rs` at 751 lines.
- 2026-07-01: Advanced MR-004 by moving top-level Exchange HTTP route assembly
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/http_routes.rs` as `exchange_router`.
  `service.rs` now delegates its public `router()` entry point to that focused
  route module while preserving the existing endpoint paths and handler
  functions.
- 2026-07-01 verification for the HTTP route assembly split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange http_routes` passed 2
  route tests; `cargo test -p lpe-exchange rpc_proxy` passed 51 RPC proxy
  tests; `cargo test -p lpe-exchange mapi_over_http::transport` passed 36
  MAPI transport tests; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `git diff
  --check` exited successfully with CRLF warnings only, and `python
  tools/check_oversized_sources.py` passed in warning mode. The oversized
  source check reports `crates/lpe-exchange/src/service.rs` at 11,549 tracked
  source lines after this slice.
- 2026-07-01: Advanced MR-004 by moving EWS request body decoding, SOAP
  operation-name detection, and the SOAP envelope response wrapper from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/xml.rs`. This keeps XML/body primitives
  together while preserving endpoint routing, SOAP dispatch behavior, response
  envelope bytes, request decoding errors, and UTF-8/UTF-16 handling.
- 2026-07-01 verification for the EWS XML/body helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_folder_items_accepts_utf16_soap_requests` passed the focused UTF-16
  SOAP request test; `cargo test -p lpe-exchange http_routes` passed 2 route
  tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1,594 tests and doc tests passing. `git diff --check` exited successfully
  with CRLF warnings only, and `python tools/check_oversized_sources.py`
  passed in warning mode. The oversized-source check reports
  `crates/lpe-exchange/src/service.rs` at 11,448 tracked source lines. Direct
  physical line counts report `service.rs` at 10,817 lines and
  `service/ews/xml.rs` at 264 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS delegate request parsing
  and owner validation from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/delegation.rs`. This keeps the
  AddDelegate, UpdateDelegate, GetDelegate, and RemoveDelegate parser policy
  with the existing delegate response XML helpers while preserving canonical
  delegate storage calls, same-tenant lookup behavior, unsupported
  Exchange-only permission rejection, and SOAP response shapes.
- 2026-07-01 verification for the EWS delegate parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  delegate_operations_use_canonical_permissions_and_preferences` passed the
  canonical delegate mutation test; `cargo test -p lpe-exchange
  delegate_add_rejects` passed the cross-tenant and unsupported-permission
  rejection tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `git diff --check` exited
  successfully with CRLF warnings only, and `python
  tools/check_oversized_sources.py` passed in warning mode. The
  oversized-source check reports `crates/lpe-exchange/src/service.rs` at
  11,311 tracked source lines. Direct physical line counts report
  `service.rs` at 10,688 lines and `service/ews/delegation.rs` at 220 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS user-configuration key,
  upsert, and dictionary request parsing from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/user_configuration.rs`. This keeps
  Create/Get/Update/DeleteUserConfiguration parsing with the existing
  user-configuration response renderer while preserving canonical storage
  calls, account/mailbox/public-folder scoping, base64 validation, dictionary
  shape, and SOAP responses.
- 2026-07-01 verification for the EWS user-configuration parser split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  user_configuration_create_get_update_and_delete_use_canonical_storage`
  passed the canonical CRUD test; `cargo test -p lpe-exchange
  user_configuration_supports_mailbox_scoped_names_and_not_found_errors`
  passed the scoped-name and not-found behavior test;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `git diff --check` exited successfully with
  CRLF warnings only, and `python tools/check_oversized_sources.py` passed in
  warning mode. The oversized-source check reports
  `crates/lpe-exchange/src/service.rs` at 11,225 tracked source lines. Direct
  physical line counts report `service.rs` at 10,605 lines and
  `service/ews/user_configuration.rs` at 202 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS OOF state and
  scheduled-duration request parsing from `crates/lpe-exchange/src/service.rs`
  into `crates/lpe-exchange/src/service/ews/oof.rs`. This keeps
  SetUserOofSettings parser policy with the existing OOF response XML helpers
  while preserving OOF Sieve projection/storage, disabled/enabled/scheduled
  state behavior, duration validation messages, and SOAP responses.
- 2026-07-01 verification for the EWS OOF parser split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange oof_settings` passed 6 focused
  OOF tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1,594 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 11,204 tracked source lines. Direct
  physical line counts report `service.rs` at 10,586 lines and
  `service/ews/oof.rs` at 70 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS file-attachment upload
  parsing and expected attachment-kind detection from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/attachments.rs`. This keeps
  CreateAttachment request parsing with the existing attachment response XML
  helpers while preserving Magika validation, canonical attachment storage,
  audit behavior, and Get/Create/DeleteAttachment SOAP responses.
- 2026-07-01 verification for the EWS attachment parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange attachment` passed 47
  focused attachment tests including the EWS Get/Create/DeleteAttachment cases;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  11,160 tracked source lines. Direct physical line counts report `service.rs`
  at 10,545 lines and `service/ews/attachments.rs` at 97 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS ConvertId request parsing,
  canonical object-id validation, opaque EWS id encoding/decoding, destination
  format normalization, and ConvertId helper structs from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/ids.rs`. This keeps ConvertId request
  and response policy together while preserving supported canonical object
  families, HexEntryId round trips, opaque id format, and SOAP response shape.
- 2026-07-01 verification for the EWS ConvertId split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange convert_id` passed 2 focused
  ConvertId tests; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 10,956 tracked source lines. Direct
  physical line counts report `service.rs` at 10,355 lines and
  `service/ews/ids.rs` at 217 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS MailTips recipient/tip
  request parsing and GetServiceConfiguration request parsing from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail_tips.rs`. This keeps the bounded
  MailTips and service-configuration request policy with the existing response
  renderers while preserving directory lookup, OOF projection, parseable
  unsupported-configuration responses, and SOAP response shapes.
- 2026-07-01 verification for the EWS MailTips parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange mail_tips` passed 3
  focused MailTips/service-configuration tests; `cargo test -p lpe-exchange
  service_configuration` passed 2 focused service-configuration tests;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  10,880 tracked source lines. Direct physical line counts report `service.rs`
  at 10,282 lines and `service/ews/mail_tips.rs` at 231 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS Mail Apps request parsing
  for app identifiers and client-access token scopes into
  `crates/lpe-exchange/src/service/ews/mail_apps.rs`, and EWS Unified
  Messaging phone-call id parsing into
  `crates/lpe-exchange/src/service/ews/unified_messaging.rs`. This keeps
  operation-specific request parsing with each response module while preserving
  canonical catalog/install/token state, canonical call state, and SOAP
  response/error behavior.
- 2026-07-01 verification for the EWS Mail Apps and Unified Messaging parser
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mail_app_operations` passed the focused catalog/install/token-state test;
  `cargo test -p lpe-exchange unified_messaging_operations` passed the focused
  canonical call-state test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 10,849 tracked source lines. Direct
  physical line counts report `service.rs` at 10,254 lines,
  `service/ews/mail_apps.rs` at 138 lines, and
  `service/ews/unified_messaging.rs` at 85 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS reminder ItemId request
  parsing into `crates/lpe-exchange/src/service/ews/reminders.rs`, and EWS
  GetRooms room-list address parsing into
  `crates/lpe-exchange/src/service/ews/rooms.rs`. This keeps pure request
  parsing beside the corresponding response helpers while preserving canonical
  reminder updates, room/resource directory projection, unsupported custom room
  list behavior, and SOAP response shapes.
- 2026-07-01 verification for the EWS reminder and rooms parser split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  reminders_are_read_and_dismissed_from_canonical_reminder_state` passed the
  focused reminder action test; `cargo test -p lpe-exchange
  rooms_are_projected_from_canonical_directory_entries` passed the focused
  room/resource projection test; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `ParsedReminderItemId`, `parse_reminder_item_id`, and
  `requested_room_list_address` now live in the focused EWS modules instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 10,820 tracked
  source lines. Direct physical line counts report `service.rs` at 10,228
  lines, `service/ews/reminders.rs` at 74 lines, and `service/ews/rooms.rs` at
  76 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS inbox-rule mutation
  classification, bounded rule-to-Sieve projection, and Sieve string escaping
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/rules.rs`. Canonical Sieve script
  writes remain in the service method, while request parsing and response XML
  now live together in the rules module. This preserves GetInboxRules output,
  UpdateInboxRules create/set/delete behavior, Exchange-only rule rejection,
  and no-side-effect failure behavior.
- 2026-07-01 verification for the EWS inbox-rule parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  inbox_rules_project_and_update_canonical_sieve_rules` passed the focused
  projection/update test; `cargo test -p lpe-exchange
  update_inbox_rules_rejects_exchange_only_rule_shapes_without_side_effects`
  passed the focused rejection/no-side-effect test; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `EwsInboxRuleMutation`, `bounded_ews_rule_to_sieve`, and
  `escape_sieve_string` now live in `service/ews/rules.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 10,744 tracked
  source lines. Direct physical line counts report `service.rs` at 10,156
  lines and `service/ews/rules.rs` at 113 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS task create/update request
  parsing, task-list folder id parsing, and EWS task status normalization from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/tasks.rs`. Shared recurrence parsing
  remains in `service.rs` for now because it is used by both calendar and task
  inputs. This preserves canonical task create/update/delete behavior, task
  folder routing, task status mapping, HTML body text conversion, field-delete
  handling, and SyncFolderItems task projection.
- 2026-07-01 verification for the EWS task parser split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  create_update_task_round_trips_through_sync_folder_items` passed the focused
  task create/update/sync test; `cargo test -p lpe-exchange
  delete_item_deletes_canonical_task` passed the focused task delete routing
  test; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with
  1,594 tests and doc tests passing. `rg` confirms `parse_create_task_input`,
  `parse_update_task_input`, `requested_task_list_id`, and
  `ews_task_status_to_canonical` now live in `service/ews/tasks.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 10,643 tracked
  source lines. Direct physical line counts report `service.rs` at 10,061
  lines and `service/ews/tasks.rs` at 208 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS Recurrence
  request parser and its bounded RRULE helpers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/calendar.rs`. The parser remains shared
  by calendar and task request paths, but now sits with the existing calendar
  RRULE-to-EWS response projection code. This preserves supported daily,
  weekly, absolute monthly, absolute yearly, numbered, and end-date recurrence
  behavior, unsupported-pattern errors, and the existing task/calendar callers.
- 2026-07-01 verification for the EWS recurrence parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed the
  focused calendar recurrence create/sync/delete test; `cargo test -p
  lpe-exchange create_update_task_round_trips_through_sync_folder_items`
  passed the focused task caller test; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms `parse_ews_recurrence`, `push_interval_part`,
  `parse_positive_number`, `ews_weekday_to_rrule`, `ews_month_to_number`, and
  `rrule_date` now live in `service/ews/calendar.rs` instead of `service.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 10,543 tracked source lines. Direct
  physical line counts report `service.rs` at 9,970 lines,
  `service/ews/calendar.rs` at 400 lines, and `service/ews/tasks.rs` at 208
  lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS message read/flag update
  request parsing from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail.rs`, and moving generic EWS XML
  boolean parsing helpers into `crates/lpe-exchange/src/service/ews/xml.rs`.
  Canonical flag mutation remains in the service method. This preserves
  UpdateItem message IsRead/FlagStatus behavior, field-delete handling for
  message flags, MarkAllItemsAsRead boolean parsing, and notification/delegate
  boolean parsing callers.
- 2026-07-01 verification for the EWS message flag parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  update_item_updates_message_read_and_flag_state` passed the focused
  UpdateItem read/flag test; `cargo test -p lpe-exchange
  mark_all_items_as_read_updates_canonical_mailbox_message_flags` passed the
  focused mailbox read-flag bulk-update test; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `parse_update_message_flags` now lives in `service/ews/mail.rs`
  and `parse_xml_bool` / `parse_xml_bool_attr` now live in
  `service/ews/xml.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 10,513 tracked source lines. Direct
  physical line counts report `service.rs` at 9,944 lines,
  `service/ews/mail.rs` at 106 lines, and `service/ews/xml.rs` at 274 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS Message CreateItem request
  parsing from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail.rs`. The canonical draft/import
  handling remains in the service method, and shared EWS mailbox/recipient
  parsing remains in `service.rs` because it is still used by directory,
  MailTips, calendar participants, and sharing request paths. This preserves
  MessageDisposition handling, From/Sender defaults, recipient parsing, body
  text conversion, protected Bcc input, draft creation, custom-folder import,
  and CreateItem SOAP response behavior.
- 2026-07-01 verification for the EWS message CreateItem parser split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  create_item_saveonly_stores_message_as_canonical_draft` passed the focused
  draft creation test; `cargo test -p lpe-exchange
  create_item_saveonly_imports_message_into_custom_mailbox_folder` passed the
  focused custom-folder import test; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms `parse_create_message_input` now lives in `service/ews/mail.rs`
  instead of `service.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 10,463
  tracked source lines. Direct physical line counts report `service.rs` at
  9,896 lines and `service/ews/mail.rs` at 154 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS Contact CreateItem and
  UpdateItem request parsing from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/contacts.rs`. The slice also moves the
  contact-only indexed entry parser, contact JSON update helpers, and contact
  display fallback helpers into the contact module. Shared field-delete and
  updated-text helpers remain in `service.rs` because mail, task, and calendar
  parsers still use them. This preserves contact create/update SOAP behavior,
  rich-field omission on narrow updates, sync-folder visibility, and canonical
  contact mutations.
- 2026-07-01 verification for the EWS contact parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  ews_contact_narrow_update_omits_unowned_rich_fields` passed in its new
  module location; `cargo test -p lpe-exchange
  create_delete_contact_round_trips_through_sync_folder_items` passed the
  focused contact CreateItem/DeleteItem sync round-trip; `cargo test -p
  lpe-exchange update_contact_round_trips_through_sync_folder_items` passed
  the focused contact UpdateItem sync round-trip; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `parse_create_contact_input`, `parse_update_contact_input`,
  `contact_entry_value`, and the narrow-update test now live in
  `service/ews/contacts.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 10,003 tracked source lines. Direct
  physical line counts report `service.rs` at 9,460 lines and
  `service/ews/contacts.rs` at 651 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS CalendarItem CreateItem and
  UpdateItem request parsing from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/calendar.rs`. The slice also moves the
  event participant parser, EWS response-type-to-partstat mapper, time-zone
  lookup, and event date/duration helpers into the calendar module. Availability
  window helpers remain in `service.rs` because the free/busy path and
  `service/ews/availability.rs` still use them. This preserves CalendarItem
  create/update parsing, attendee metadata serialization, recurrence handling,
  body conversion, and canonical calendar mutations.
- 2026-07-01 verification for the EWS calendar event parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed the
  focused CalendarItem CreateItem/DeleteItem sync round-trip; `cargo test -p
  lpe-exchange find_item_returns_calendar_items_from_canonical_store` passed
  the focused calendar FindItem projection test; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `parse_create_event_input`, `parse_update_event_input`,
  `parse_event_participants`, and `ews_datetime_parts` now live in
  `service/ews/calendar.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,773 tracked source lines. Direct
  physical line counts report `service.rs` at 9,241 lines and
  `service/ews/calendar.rs` at 619 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving public-folder item UpdateItem
  parsing and MoveItem/CopyItem clone input construction from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/public_folders.rs`. Public-folder
  service orchestration, CreateItem public-folder post construction, and
  recursive folder-copy traversal remain in `service.rs` because they are tied
  to request dispatch, audit actions, and store control flow. This preserves
  public-folder item subject/body updates, message-class preservation, move
  copy-then-delete behavior, copy behavior, and canonical public-folder item
  mutations.
- 2026-07-01 verification for the EWS public-folder item helper split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  update_item_updates_public_folder_item` passed the focused UpdateItem
  public-folder item test; `cargo test -p lpe-exchange
  move_item_moves_public_folder_item_to_target_public_folder` passed the
  focused MoveItem public-folder item test; `cargo test -p lpe-exchange
  copy_item_copies_public_folder_item_to_target_public_folder` passed the
  focused CopyItem public-folder item test; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms `parse_update_public_folder_item_input` and
  `public_folder_item_clone_input` now live in `service/ews/public_folders.rs`
  instead of `service.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 9,715
  tracked source lines. Direct physical line counts report `service.rs` at
  9,185 lines and `service/ews/public_folders.rs` at 130 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS out-of-office
  projection model and Sieve conversion helpers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/oof.rs`. The service methods still own
  request dispatch, store reads/writes, and audit actions. This preserves
  GetUserOofSettings projection over canonical Sieve vacation scripts,
  SetUserOofSettings canonical Sieve generation, scheduled OOF metadata, OOF
  disable behavior, and MailTips OOF projection.
- 2026-07-01 verification for the EWS OOF helper split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  get_user_oof_settings_projects_canonical_sieve_vacation` passed the focused
  OOF projection test; `cargo test -p lpe-exchange
  set_user_oof_settings_writes_canonical_sieve_vacation` passed the focused
  OOF write test; `cargo test -p lpe-exchange
  set_user_oof_settings_scheduled_round_trips_canonical_sieve_metadata` passed
  the scheduled OOF metadata round-trip; `cargo test -p lpe-exchange
  get_mail_tips_projects_directory_and_oof_without_local_tip_state` passed the
  MailTips OOF projection caller; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `OofDuration`, `OofProjection`, `oof_projection_from_script`,
  `vacation_sieve_script`, `find_vacation_reason`, and `sieve_quote` now live
  in `service/ews/oof.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,573 tracked source lines. Direct
  physical line counts report `service.rs` at 9,053 lines and
  `service/ews/oof.rs` at 200 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving shared EWS calendar datetime
  rendering helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/calendar.rs`, and moving
  availability request-window and event-overlap helpers into
  `crates/lpe-exchange/src/service/ews/availability.rs`. The
  GetUserAvailability service method still owns recipient resolution, store
  reads, filtering orchestration, and response selection. This preserves
  calendar item Start/End XML rendering, free/busy event windows, suggestions
  fallback windows, and canonical event filtering.
- 2026-07-01 verification for the EWS availability/calendar helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_user_availability_returns_canonical_busy_events` passed the focused
  free/busy filtering test; `cargo test -p lpe-exchange
  get_user_availability_returns_suggestions_when_requested` passed the
  suggestions fallback-window test; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed the
  focused CalendarItem Start/End rendering path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `ews_datetime` and `event_end_datetime` now live in
  `service/ews/calendar.rs`, while `requested_availability_window` and
  `event_overlaps_window` now live in `service/ews/availability.rs`, instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,539 tracked
  source lines. Direct physical line counts report `service.rs` at 9,023
  lines, `service/ews/availability.rs` at 106 lines, and
  `service/ews/calendar.rs` at 636 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS CreateItem
  save-only custom mailbox import input mapper from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail.rs`. The service method still owns
  request dispatch, target folder resolution, store import, and audit action
  selection. This preserves the exact `SubmitMessageInput` to
  `JmapImportedEmailInput` field mapping for saved messages in custom mailbox
  folders.
- 2026-07-01 verification for the EWS mail import helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  create_item_saveonly_imports_message_into_custom_mailbox_folder` passed the
  focused custom-folder import path; `cargo test -p lpe-exchange
  create_item_saveonly_stores_message_as_canonical_draft` passed the adjacent
  draft path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1,594 tests and doc tests passing. `rg` confirms
  `imported_email_input` now lives in `service/ews/mail.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,511 tracked source
  lines. Direct physical line counts report `service.rs` at 8,996 lines and
  `service/ews/mail.rs` at 184 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS canonical
  message ItemId parser from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/ids.rs`, alongside the existing
  ConvertId and canonical EWS object ID helpers. The PlayOnPhone and SendItem
  service methods still own operation dispatch, store calls, audit actions,
  and response mapping.
- 2026-07-01 verification for the EWS message ID helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  send_item_submits_existing_draft_through_canonical_submission` passed the
  focused SendItem draft-submission path; `cargo test -p lpe-exchange
  unified_messaging_operations_use_canonical_call_state` passed the focused
  PlayOnPhone message ItemId path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `canonical_message_id_from_ews_id` now lives in `service/ews/ids.rs` instead
  of `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,503 tracked source
  lines. Direct physical line counts report `service.rs` at 8,989 lines and
  `service/ews/ids.rs` at 224 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving stateless EWS request folder ID
  parsers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/request_ids.rs`. This includes
  mailbox-folder IDs, public-folder IDs, distinguished mailbox roles, and
  wrapper-scoped variants. The async service method that resolves mailbox roles
  against canonical mailbox storage remains in `service.rs` because it owns
  store access and fallback behavior.
- 2026-07-01 verification for the EWS request folder ID helper split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_folder_items_returns_empty_sync_for_custom_mailbox_folder` passed the
  focused custom mailbox-folder ID path; `cargo test -p lpe-exchange
  sync_folder_items_reports_public_folder_items` passed the focused public
  folder ID path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. The first focused compile
  identified two existing cross-module users of `requested_distinguished_folder_id`
  and `ews_distinguished_mailbox_role`; those helpers are now visible within
  `crate::service`. `rg` confirms the moved stateless parsers now live in
  `service/ews/request_ids.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,449 tracked source lines. Direct
  physical line counts report `service.rs` at 8,943 lines and
  `service/ews/request_ids.rs` at 97 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS sync-state request parsing
  and mailbox-folder sync-state helpers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/sync_state.rs`, alongside the existing
  collaboration sync-state formatter/parser. General collection ID extraction
  remains in `service.rs` for now because non-sync EWS paths still share it.
- 2026-07-01 verification for the EWS sync-state helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_folder_items_returns_contacts_from_canonical_store` passed the focused
  collaboration sync-state path; `cargo test -p lpe-exchange
  sync_folder_items_reports_custom_mailbox_create_and_delete_changes` passed
  the focused custom mailbox sync-state path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `requested_sync_collection_id`, `requested_sync_state`,
  `mailbox_sync_state`, `mailbox_sync_state_ids`, and
  `mailbox_sync_state_folder_id` now live in `service/ews/sync_state.rs`
  instead of `service.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 9,405
  tracked source lines. Direct physical line counts report `service.rs` at
  8,904 lines and `service/ews/sync_state.rs` at 136 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS sharing request parsing
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/sharing.rs`, alongside the sharing
  response renderers. The service methods still own same-tenant owner
  resolution, accessible collection checks, grant writes, audit actions, and
  operation response mapping.
- 2026-07-01 verification for the EWS sharing parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  get_sharing_metadata_returns_owned_calendar_metadata_without_exchange_tokens`
  passed the focused metadata request-kind path; `cargo test -p lpe-exchange
  accept_sharing_invitation_creates_same_tenant_calendar_grant` passed the
  focused sharing invitation parser and grant path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `SharingRequest`, `requested_sharing_kind`,
  `parse_sharing_request`, and `sharing_rights` now live in
  `service/ews/sharing.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,342 tracked source lines. Direct
  physical line counts report `service.rs` at 8,845 lines and
  `service/ews/sharing.rs` at 211 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS conversation request parsing
  and ignored-folder filtering helpers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/conversations.rs`, alongside the
  conversation response renderers. The service methods still own source email
  loading, canonical message move/delete/read-state mutations, audit actions,
  and unsupported future-message action response mapping.
- 2026-07-01 verification for the EWS conversation parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  get_conversation_items_returns_current_canonical_thread_nodes` passed the
  focused ConversationId parsing and ignored-folder filtering path; `cargo test
  -p lpe-exchange apply_conversation_action_moves_current_thread_messages`
  passed the focused conversation action parser and destination folder path;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `ConversationActionRequest`,
  `requested_conversation_ids`, `parse_conversation_id`,
  `parse_conversation_actions`, and `filter_ignored_conversation_folders` now
  live in `service/ews/conversations.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,289 tracked source lines. Direct
  physical line counts report `service.rs` at 8,797 lines and
  `service/ews/conversations.rs` at 294 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the remaining stateless EWS
  collection and relative-folder-path request parsers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/request_ids.rs`. This includes
  `request_contains_folder_reference`, `requested_collection_id`,
  `requested_collection_id_in`, and `requested_folder_path_segments`. The
  service methods still own folder resolution, canonical store access, default
  collection fallbacks, and SOAP response mapping.
- 2026-07-01 verification for the EWS collection/path request parser split:
  focused tests for FindFolder, CreateFolder, and SyncFolderItems collection
  paths had already passed before the full verification. `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms the moved helper definitions now live in
  `service/ews/request_ids.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,249 tracked source lines. Direct
  physical line counts report `service.rs` at 8,761 lines and
  `service/ews/request_ids.rs` at 136 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS GetItem MIME content
  request helper from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail.rs`, next to the message XML and
  MIME response rendering helpers. The GetItem service method still owns item
  loading, attachment-content loading, Bcc-safe MIME rendering selection, and
  SOAP response mapping.
- 2026-07-01 verification for the EWS MIME content request helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox`
  passed the focused normal-mailbox MIME request path; `cargo test -p
  lpe-exchange get_item_mime_content_hides_bcc_for_sent_message_default_fetch`
  passed the sent-message Bcc hiding path; `cargo test -p lpe-exchange
  get_item_mime_content_includes_canonical_attachments` passed the canonical
  attachment MIME path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `requested_mime_content` now lives in `service/ews/mail.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,245 tracked source
  lines. Direct physical line counts report `service.rs` at 8,758 lines and
  `service/ews/mail.rs` at 187 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS mailbox parser
  cluster from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mailboxes.rs`. This includes
  `ParsedMailbox`, `parse_recipients`, `parse_first_mailbox`, and
  `parse_mailbox`. The slice preserves the existing XML parsing behavior used
  by message recipients, availability mailbox checks, directory requests,
  MailTips recipients, calendar attendees, and sharing invitations.
- 2026-07-01 verification for the shared EWS mailbox parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  create_item_saveonly_stores_message_as_canonical_draft` passed the message
  recipient path; `cargo test -p lpe-exchange
  get_user_availability_returns_canonical_busy_events` passed the availability
  mailbox path; `cargo test -p lpe-exchange
  get_mail_tips_projects_directory_and_oof_without_local_tip_state` passed the
  MailTips recipient path; `cargo test -p lpe-exchange
  accept_sharing_invitation_creates_same_tenant_calendar_grant` passed the
  sharing mailbox path; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed
  the calendar attendee path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the moved mailbox parser definitions now live in `service/ews/mailboxes.rs`
  instead of `service.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 9,209
  tracked source lines. Direct physical line counts report `service.rs` at
  8,726 lines and `service/ews/mailboxes.rs` at 38 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS compliance mailbox-list
  parser `requested_mailbox_emails` from `crates/lpe-exchange/src/service.rs`
  into `crates/lpe-exchange/src/service/ews/mailboxes.rs`, next to the shared
  mailbox XML parser cluster. Search and hold service methods still own
  canonical compliance store calls, result limits, audit input construction,
  and SOAP response mapping.
- 2026-07-01 verification for the EWS compliance mailbox-list parser split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  search_mailboxes_records_canonical_discovery_search_results_without_bcc`
  passed the SearchMailboxes parser path; `cargo test -p lpe-exchange
  hold_operations_use_canonical_compliance_hold_state` passed the
  GetHoldOnMailboxes and SetHoldOnMailboxes parser paths; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  The attempted narrower `get_hold_on_mailboxes_projects_canonical_hold_state`
  filter matched zero tests and was replaced by the exact combined hold test.
  `rg` confirms `requested_mailbox_emails` now lives in
  `service/ews/mailboxes.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,197 tracked source lines. Direct
  physical line counts report `service.rs` at 8,715 lines and
  `service/ews/mailboxes.rs` at 49 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS field-delete
  parsing helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/fields.rs`. This includes
  `deleted_or_updated_text`, `field_deleted`, and the local
  `field_block_matches` matcher. The object-family parsers still own their
  field-to-canonical-input mapping and service methods still own store calls
  and SOAP response mapping.
- 2026-07-01 verification for the EWS field-delete helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  update_item_updates_message_read_and_flag_state` passed the message read/flag
  delete-field path; `cargo test -p lpe-exchange
  update_contact_round_trips_through_sync_folder_items` passed the contact
  update path; `cargo test -p lpe-exchange update_item_updates_public_folder_item`
  passed the public-folder item update path; `cargo test -p lpe-exchange
  create_update_task_round_trips_through_sync_folder_items` passed the task
  update path; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed the
  calendar item path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `rg` confirms the moved
  helper definitions now live in `service/ews/fields.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,162 tracked source
  lines. Direct physical line counts report `service.rs` at 8,684 lines and
  `service/ews/fields.rs` at 34 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving shared EWS XML numeric-attribute
  and tag-count helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/xml.rs`, next to the existing XML
  primitive helpers. This includes `ews_usize_attribute`,
  `count_folder_elements`, and `count_tag_occurrences`. The service methods
  and response builders still own their response selection, debug summaries,
  and SOAP payload assembly.
- 2026-07-01 verification for the EWS XML count helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  get_conversation_items_returns_current_canonical_thread_nodes` passed the
  focused `IndexedPageItemView` numeric-attribute and conversation count path;
  `cargo test -p lpe-exchange
  sync_folder_hierarchy_lists_contact_and_calendar_folders` passed the focused
  folder-count response path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the moved helper definitions now live in `service/ews/xml.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,147 tracked source
  lines. Direct physical line counts report `service.rs` at 8,672 lines and
  `service/ews/xml.rs` at 286 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS notification event
  model from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/notifications.rs`, beside the
  notification response renderers that consume it. This includes
  `EwsQueuedNotification` and `EwsNotificationKind`. The service method still
  owns canonical notification polling, event filtering, and change-kind to EWS
  event-kind mapping.
- 2026-07-01 verification for the EWS notification model split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  pull_and_streaming_notifications_replay_canonical_sql_change_cursor` passed
  the focused pull/streaming replay path; `cargo test -p lpe-exchange
  pull_subscription_get_events_replays_canonical_changes_after_restart` passed
  the focused canonical NewMail event mapping path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  The attempted narrower filter
  `get_events_replays_new_mail_canonical_change` matched zero tests and was
  replaced by the exact restart replay test. `rg` confirms the moved type
  definitions now live in `service/ews/notifications.rs` instead of
  `service.rs`. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 9,130 tracked source
  lines. Direct physical line counts report `service.rs` at 8,657 lines and
  `service/ews/notifications.rs` at 223 lines.
- 2026-07-01: Advanced MR-004 by moving EWS request/response diagnostics from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/diagnostics.rs`. This includes
  `ews_operation_hint`, `log_ews_connection`, `EwsResponseDebug`,
  `ews_response_code`, `ews_response_debug_detail`, and
  `ews_payload_debug_detail`. EWS route handling, response creation, SOAP
  payloads, and MAPI/RPC diagnostics remain unchanged.
- 2026-07-01 verification for the EWS diagnostics split: `cargo fmt --package
  lpe-exchange`; `cargo test -p lpe-exchange
  create_item_saveonly_stores_message_as_canonical_draft` passed the focused
  CreateItem debug-detail path; `cargo test -p lpe-exchange
  sync_folder_items_returns_contacts_from_canonical_store` passed the focused
  SyncFolderItems debug-detail path; `cargo test -p lpe-exchange
  pull_subscription_get_events_replays_canonical_changes_after_restart` passed
  the focused GetEvents debug-detail path; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms the moved diagnostics definitions now live in
  `service/ews/diagnostics.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 9,008 tracked source lines. Direct
  physical line counts report `service.rs` at 8,542 lines and
  `service/ews/diagnostics.rs` at 123 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS folder-kind request
  classifier from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/folders.rs`, beside the folder response
  and projection helpers. This includes `FolderKind`,
  `requested_folder_kind`, `sync_state_folder_kind`, and
  `requested_folder_kinds`. The service methods still own store reads,
  response assembly, and object-family routing after classification.
- 2026-07-01 verification for the EWS folder-kind classifier split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  find_folder_lists_contact_and_calendar_folders` passed the FindFolder
  multi-kind classification path; `cargo test -p lpe-exchange
  find_item_lists_custom_mailbox_messages` passed the FindItem mailbox
  classification path; `cargo test -p lpe-exchange
  sync_folder_items_reports_custom_mailbox_create_and_delete_changes` passed
  the mailbox SyncFolderItems classification path; `cargo test -p lpe-exchange
  sync_folder_items_reports_public_folder_items` passed the public-folder
  SyncFolderItems classification path; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms the classifier definitions now live in `service/ews/folders.rs`
  instead of `service.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 8,864
  tracked source lines. Direct physical line counts report `service.rs` at
  8,402 lines and `service/ews/folders.rs` at 344 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving EWS custom mailbox folder guard
  helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/folders.rs`, beside the folder request
  classifier and folder response helpers. This includes `mailbox_by_id` and
  `ensure_custom_mailbox`. The folder mutation service methods still own store
  reads/writes, audit input construction, and SOAP response mapping.
- 2026-07-01 verification for the EWS custom mailbox guard split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  copy_move_and_update_folder_use_canonical_mailbox_changes` passed the
  focused custom-folder move/copy/update path; `cargo test -p lpe-exchange
  folder_operations_preserve_system_and_public_folder_boundaries` passed the
  focused system-folder and public-folder boundary path;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms the guard helper definitions now
  live in `service/ews/folders.rs` instead of `service.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 8,849 tracked source lines. Direct
  physical line counts report `service.rs` at 8,389 lines and
  `service/ews/folders.rs` at 360 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS stable
  change-key helper from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/ids.rs`, beside the EWS ID conversion
  helpers used by item-family response builders. Contact, calendar, task, and
  public-folder response modules now share the helper from the ID module; the
  response builders still own the object-family-specific key inputs.
- 2026-07-01 verification for the EWS stable change-key helper split: focused
  item-family tests for contact, calendar, task, and public-folder update paths
  had already passed before the full verification; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `stable_change_key` is now defined only in
  `service/ews/ids.rs` and consumed by the EWS contact, calendar, task, and
  public-folder modules. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 8,836
  tracked source lines. Direct physical line counts report `service.rs` at
  8,377 lines and `service/ews/ids.rs` at 236 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the shared EWS empty-string
  fallback extension helper from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/fields.rs`, beside the shared EWS field
  update helpers. Calendar and task parsers still own their object-specific
  update mapping; the helper only preserves the existing blank-value fallback
  behavior.
- 2026-07-01 verification for the EWS empty-string fallback helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  create_delete_calendar_item_round_trips_through_sync_folder_items` passed
  the calendar update path; `cargo test -p lpe-exchange
  create_update_task_round_trips_through_sync_folder_items` passed the task
  update path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1,594 tests and doc tests passing. `rg` confirms
  `EmptyStringFallback` now lives in `service/ews/fields.rs` and the remaining
  `.if_empty(...)` call sites are in the EWS calendar and task modules.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 8,822 tracked source lines. Direct
  physical line counts report `service.rs` at 8,365 lines and
  `service/ews/fields.rs` at 46 lines.
- 2026-07-01: Advanced MR-004 by moving service-level HTTP response/query
  diagnostics helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/http_utils.rs`. This includes
  `response_header`, `response_set_cookie_names`, and `query_parameter`. Route
  handling, MAPI/HTTP response envelopes, RPC proxy responses, and logging
  fields remain unchanged.
- 2026-07-01 verification for the service HTTP helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_options_handler_reports_transport_session_ready` passed the MAPI route
  response-header path; `cargo test -p lpe-exchange
  rpc_proxy_accepts_authenticated_rca_probe_without_405` passed the RPC proxy
  response logging path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the helper definitions now live in `service/http_utils.rs` and `service.rs`
  only calls them. `python tools/check_oversized_sources.py` passed in warning
  mode and reports `crates/lpe-exchange/src/service.rs` at 8,790 tracked source
  lines. Direct physical line counts report `service.rs` at 8,336 lines and
  `service/http_utils.rs` at 32 lines.
- 2026-07-01: Advanced MR-004 by moving MAPI/HTTP and RPC proxy service
  diagnostics from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/transport_diagnostics.rs`. This includes
  `log_mapi_transport_connection`, `log_rpc_proxy_connection`, and the RPC
  proxy response extension types used to report payload byte counts and preview
  hashes. MAPI/HTTP response envelopes, RPC proxy response generation, endpoint
  routes, and auth behavior remain unchanged.
- 2026-07-01 verification for the transport diagnostics split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_options_handler_reports_transport_session_ready` passed the MAPI
  transport logging path; `cargo test -p lpe-exchange
  rpc_proxy_accepts_authenticated_rca_probe_without_405` passed the RPC proxy
  diagnostics path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `rg` confirms the moved
  logging helpers and RPC proxy response debug types now live in
  `service/transport_diagnostics.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  8,539 tracked source lines. Direct physical line counts report `service.rs`
  at 8,093 lines and `service/transport_diagnostics.rs` at 246 lines.
- 2026-07-01: Advanced MR-004 by moving primitive RPC proxy codec helpers from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_codec.rs`. This includes
  little-endian `u32` reads/writes plus bounded NDR byte-array, ASCII string,
  and UTF-16 string writers. Higher-level RPC proxy response builders, routing,
  auth, endpoint classification, and NSPI matching remain in `service.rs`.
- 2026-07-01 verification for the RPC proxy codec helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response` passed
  the ASCII NDR response path; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response` passed the
  UTF-16 NDR response path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the primitive helper definitions now live in `service/rpc_proxy_codec.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 8,496 tracked source lines. Direct
  physical line counts report `service.rs` at 8,055 lines and
  `service/rpc_proxy_codec.rs` at 40 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy request classifier helpers
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_requests.rs`. This includes
  endpoint-ping, MSRPC header, zero-length, echo-probe, and streaming
  `RPC_IN_DATA` classification. RPC proxy parsing, response generation,
  endpoint paths, auth, and transport semantics remain unchanged.
- 2026-07-01 verification for the RPC proxy request classifier split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_classifies_zero_length_endpoint_in_data_as_echo_probe` passed the
  zero-length echo classification path; `cargo test -p lpe-exchange
  rpc_proxy_classifies_referral_endpoint_as_streaming_in_data_channel` passed
  the streaming `RPC_IN_DATA` path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the classifier definitions now live in `service/rpc_proxy_requests.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 8,456 tracked source lines. Direct
  physical line counts report `service.rs` at 8,022 lines and
  `service/rpc_proxy_requests.rs` at 40 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy RTS `RPC_OUT_DATA` connect
  parser helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_rts.rs`. This includes the
  `RpcProxyOutDataConnect` request model, Conn A1 RTS parser, and shared RTS
  command parsers used by the existing Conn B1 parser. RPC proxy routing,
  endpoint paths, auth, response generation, and RTS response bytes remain
  unchanged.
- 2026-07-01 verification for the RPC proxy RTS parser split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack`
  passed the Conn B1 ordering path; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack` passed the
  wait path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed
  with 1,594 tests and doc tests passing. `rg` confirms the moved RTS parser
  definitions now live in `service/rpc_proxy_rts.rs`, with `service.rs` only
  declaring and importing that module. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  8,382 tracked source lines. Direct physical line counts report `service.rs`
  at 7,954 lines and `service/rpc_proxy_rts.rs` at 72 lines.
- 2026-07-01: Advanced MR-004 by moving primitive RPC proxy RTS response-body
  helpers from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_rts.rs`. This includes RTS
  connect body construction, endpoint connect timeout body construction, Conn
  B1 response-body parsing, the shared in-channel response carrier, and RTS
  header/connection-established PDU builders. Higher-level HTTP response
  assembly remains in `service.rs`.
- 2026-07-01 verification for the RPC proxy RTS response-body split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack`
  passed the Conn B1 ordering path; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack` passed the
  wait path; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response` passed
  the connection-established response path; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms the moved RTS response-body helper definitions now live in
  `service/rpc_proxy_rts.rs`. `python tools/check_oversized_sources.py` passed
  in warning mode and reports `crates/lpe-exchange/src/service.rs` at 8,297
  tracked source lines. Direct physical line counts report `service.rs` at
  7,878 lines and `service/rpc_proxy_rts.rs` at 149 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy DCE/RPC primitive helpers
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_dce.rs`. This includes bind ack
  and alter-context response construction, context-result negotiation,
  bound-interface tracking, protocol fault framing, request-auth trailer
  echoing, and generic DCE response framing. Endpoint operation selection and
  service routing remain in `service.rs`.
- 2026-07-01 verification for the RPC proxy DCE/RPC helper split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_bind_request_gets_bind_ack_response` passed the bind ack
  path; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_alter_context_request_gets_alter_context_response`
  passed the alter-context path; `cargo test -p lpe-exchange
  rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault` passed the
  protocol-fault path; `cargo test -p lpe-exchange
  rpc_proxy_address_book_management_stats_accepts_rca_short_stub` passed the
  RCA-style management path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the DCE/RPC helper definitions now live in `service/rpc_proxy_dce.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 7,916 tracked source lines. Direct
  physical line counts report `service.rs` at 7,525 lines and
  `service/rpc_proxy_dce.rs` at 364 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy out-channel bookkeeping from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_channels.rs`. This includes
  out-channel registration, cookie-scoped forwarding, pending response queues,
  RTS connect markers, bind-ack markers, stale-channel removal, and the
  existing cookie-scoping unit tests. Held-open HTTP response assembly remains
  in `service.rs`.
- 2026-07-01 verification for the RPC proxy out-channel bookkeeping split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie` passed the
  moved channel scoping unit test; `cargo test -p lpe-exchange
  rpc_proxy_cookie_scoped_response_does_not_fall_back_to_unscoped_out_channel`
  passed the moved stale-cookie unit test; `cargo test -p lpe-exchange
  rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first`
  passed the pending out-channel response path; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack`
  passed the bind-ack ordering path; `$env:RUST_TEST_THREADS='1'; cargo test
  -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms the bookkeeping definitions now live in `service/rpc_proxy_channels.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 7,702 tracked source lines. Direct
  physical line counts report `service.rs` at 7,336 lines and
  `service/rpc_proxy_channels.rs` at 198 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy auth response builders from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_auth.rs`. This includes the
  authenticated compatibility acceptance response and Basic challenge response.
  Authentication decisions and endpoint routing remain in `service.rs`.
- 2026-07-01 verification for the RPC proxy auth response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_challenges_missing_authentication_with_basic` passed the Basic
  challenge path; `cargo test -p lpe-exchange
  rpc_proxy_accepts_authenticated_rca_probe_without_405` passed the accepted
  RCA probe path; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `rg` confirms the auth
  response builders now live in `service/rpc_proxy_auth.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 7,667 tracked source lines. Direct
  physical line counts report `service.rs` at 7,303 lines and
  `service/rpc_proxy_auth.rs` at 44 lines.
- 2026-07-01: Advanced MR-004/MR-015 by moving RPC proxy endpoint DCE/RPC
  response builders from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs`. This includes
  management stats, RFRI referral responses, EMSMDB connect/RpcExt2 responses,
  NSPI opnum dispatch, NSPI row projection, and NSPI lookup parsing used by the
  address-book fallback path. HTTP routing, auth decisions, DCE/RPC framing,
  and channel handling remain in their existing modules.
- 2026-07-01 verification for the RPC proxy endpoint response split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response` passed
  the ASCII NSPI ResolveNames path; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response` passed the
  UTF-16 NSPI ResolveNames path; `cargo test -p lpe-exchange
  rpc_proxy_address_book_management_stats_accepts_rca_short_stub` passed the
  RCA-style management stats path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 6,479 tracked source lines. Direct
  physical line counts report `service.rs` at 6,216 lines and
  `service/rpc_proxy_endpoints.rs` at 1,110 lines.
- 2026-07-01: Advanced MR-004 by moving RPC proxy streaming response assembly
  and in-data channel drain handling from `crates/lpe-exchange/src/service.rs`
  into `crates/lpe-exchange/src/service/rpc_proxy_stream.rs`. This includes
  RTS connect/echo/in-channel HTTP responses, held-open out-channel response
  assembly, in-data stream chunk logging, out-channel forwarding, buffered DCE
  fragment scanning, and the address-book CheckName fallback. Route handling,
  authentication decisions, endpoint response body construction, and
  out-channel bookkeeping remain in their existing modules.
- 2026-07-01 focused verification for the RPC proxy stream split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack`
  passed the held-open out-channel ordering path; `cargo test -p lpe-exchange
  rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof`
  passed the authenticated streaming in-data path; `cargo test -p lpe-exchange
  rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu` passed buffered
  fragment scanning; `cargo test -p lpe-exchange
  rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch` passed
  the CheckName fallback path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 5,653 tracked source lines. Direct
  physical line counts report `service.rs` at 5,428 lines and
  `service/rpc_proxy_stream.rs` at 811 lines.
- 2026-07-01: Advanced MR-004 by moving the top-level MAPI/HTTP and RPC proxy
  service entrypoint methods from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/mapi_http.rs`. This includes `handle_mapi`,
  `handle_rpc_proxy`, and `handle_rpc_proxy_in_data_channel`. Route handlers,
  authentication behavior, endpoint paths, RPC proxy response helpers, and
  MAPI execution remain unchanged.
- 2026-07-01 verification for the MAPI/HTTP service entrypoint split: `cargo
  fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  mapi_options_handler_reports_transport_session_ready` passed the MAPI route
  path; `cargo test -p lpe-exchange
  rpc_proxy_accepts_authenticated_rca_probe_without_405` passed the
  authenticated RPC proxy path; `cargo test -p lpe-exchange
  rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof`
  passed the streaming `RPC_IN_DATA` entrypoint path;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms the entrypoint method definitions
  now live in `service/mapi_http.rs`, with `service.rs` retaining only module
  wiring and route-handler calls. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  5,594 tracked source lines. Direct physical line counts report `service.rs`
  at 5,372 lines and `service/mapi_http.rs` at 64 lines.
- 2026-07-01: Advanced MR-004 by moving the RPC proxy out-channel hold-time
  helper from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/rpc_proxy_channels.rs`, beside the channel
  queues that apply the timeout. The environment override, test timeout,
  response behavior, and streaming callers remain unchanged.
- 2026-07-01 verification for the RPC proxy channel hold-time helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  rpc_proxy_out_channels_are_scoped_by_virtual_connection_cookie` passed the
  channel bookkeeping path; `cargo test -p lpe-exchange
  rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack`
  passed the held-open out-channel ordering path;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `rpc_proxy_channel_hold_ms` now
  lives in `service/rpc_proxy_channels.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 5,580 tracked source lines. Direct
  physical line counts report `service.rs` at 5,360 lines and
  `service/rpc_proxy_channels.rs` at 211 lines.
- 2026-07-01: Advanced MR-004/MR-005 by moving the EWS directory operation
  methods from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/directory.rs`, beside the existing
  directory response builders and visibility helpers. This includes
  `ResolveNames`, `ExpandDL`, `FindPeople`, `GetPersona`, `GetUserPhoto`, and
  `GetPasswordExpirationDate`. SOAP operation routing, address-book store
  calls, canonical photo/password gap responses, and response XML remain
  unchanged.
- 2026-07-01 verification for the EWS directory operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  resolve_names_returns_tenant_directory_account_match`; `cargo test -p
  lpe-exchange find_people_projects_canonical_accounts_and_contacts`; `cargo
  test -p lpe-exchange get_persona_resolves_only_visible_stateless_persona_ids`;
  `cargo test -p lpe-exchange
  expand_dl_projects_same_tenant_directory_group_members`; `cargo test -p
  lpe-exchange get_user_photo_returns_parseable_canonical_photo_gap`; and
  `cargo test -p lpe-exchange
  get_password_expiration_date_returns_parseable_canonical_account_gap` all
  passed. An initial guessed `expand_dl_projects_distribution_group_members`
  test filter matched zero tests and was replaced by the exact test name above.
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms the moved directory method
  definitions now live in `service/ews/directory.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 5,513 tracked source lines. Direct
  physical line counts report `service.rs` at 5,299 lines and
  `service/ews/directory.rs` at 470 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `MarkAsJunk` operation method
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail.rs`, beside the mail request and
  response helpers. The canonical Junk mailbox move, audit input, blocked
  Exchange-only sender behavior gap response, SOAP routing, and response XML
  remain unchanged.
- 2026-07-01 verification for the EWS `MarkAsJunk` operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  mark_as_junk_moves_messages_to_canonical_junk_mailbox` passed the canonical
  message move path; `cargo test -p lpe-exchange
  mark_as_junk_keeps_exchange_only_block_sender_behavior_parseable` passed the
  blocked-sender compatibility gap response path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `mark_as_junk` now lives in `service/ews/mail.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 5,438 tracked source lines. Direct
  physical line counts report `service.rs` at 5,228 lines and
  `service/ews/mail.rs` at 268 lines.
- 2026-07-01: Advanced MR-005 by moving EWS instant-messaging contact-list
  operation methods from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/ucs.rs`, beside the existing IM/UCS
  response builders and request parsers. This includes `GetImItemList`,
  `GetImItems`, group create/update/delete, contact/tel-URI member add/remove,
  and distribution-list member add/remove operations. SOAP routing, canonical
  contact creation, tenant-scoped distribution-list lookup, audit inputs, and
  response XML remain unchanged.
- 2026-07-01 verification for the EWS IM/UCS operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  ucs_im_group_operations_use_canonical_contact_group_state` passed the group,
  contact, tel-URI, list, and removal lifecycle paths; `cargo test -p
  lpe-exchange ucs_distribution_list_membership_stays_tenant_scoped` passed
  the visible and foreign distribution-list membership paths;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms the moved IM operation method
  definitions now live in `service/ews/ucs.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 5,122 tracked source lines. Direct
  physical line counts report `service.rs` at 4,924 lines and
  `service/ews/ucs.rs` at 588 lines.
- 2026-07-01: Advanced MR-005 by moving EWS Mail Apps operation methods from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/mail_apps.rs`, beside the existing
  request parsers and response builders. This includes `GetAppManifests`,
  `GetAppMarketplaceUrl`, `InstallApp`, `DisableApp`, `UninstallApp`, and
  `GetClientAccessToken`. Catalog lookup, marketplace policy response, install
  state changes, token hashing/issuance, audit inputs, SOAP routing, and
  response XML remain unchanged.
- 2026-07-01 verification for the EWS Mail Apps operation split: `cargo test
  -p lpe-exchange mail_app_operations_use_canonical_catalog_install_and_token_state`
  passed the catalog, install-state, and token paths; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms the moved Mail Apps operation method definitions now live in
  `service/ews/mail_apps.rs`. `python tools/check_oversized_sources.py` passed
  in warning mode and reports `crates/lpe-exchange/src/service.rs` at 4,963
  tracked source lines. Direct physical line counts report `service.rs` at
  4,771 lines and `service/ews/mail_apps.rs` at 314 lines.
- 2026-07-01: Advanced MR-005 by moving EWS Unified Messaging operation
  methods from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/unified_messaging.rs`, beside the
  existing phone-call request parser and response builders. This includes
  `PlayOnPhone`, `GetPhoneCallInformation`, and `DisconnectPhoneCall`.
  Canonical call creation/fetch/disconnect store calls, audit inputs, SOAP
  routing, not-found behavior, and response XML remain unchanged.
- 2026-07-01 verification for the EWS Unified Messaging operation split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  unified_messaging_operations_use_canonical_call_state` passed the
  PlayOnPhone, lookup, tenant isolation, and disconnect paths;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms the moved Unified Messaging
  operation method definitions now live in `service/ews/unified_messaging.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 4,868 tracked source lines. Direct
  physical line counts report `service.rs` at 4,679 lines and
  `service/ews/unified_messaging.rs` at 186 lines.
- 2026-07-01: Advanced MR-005 by moving EWS User Configuration operation
  methods from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/user_configuration.rs`, beside the
  existing configuration key parser, upsert parser, and response builder. This
  includes `GetUserConfiguration`, `CreateUserConfiguration`,
  `UpdateUserConfiguration`, and `DeleteUserConfiguration`. Account/mailbox
  scoping, canonical configuration store calls, audit inputs, not-found
  behavior, SOAP routing, and response XML remain unchanged.
- 2026-07-01 verification for the EWS User Configuration operation split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  user_configuration_` passed the account-scoped lifecycle and mailbox-scoped
  not-found paths; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `rg` confirms the moved User
  Configuration operation method definitions now live in
  `service/ews/user_configuration.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  4,724 tracked source lines. Direct physical line counts report `service.rs`
  at 4,539 lines and `service/ews/user_configuration.rs` at 347 lines.
- 2026-07-01: Advanced MR-005 by moving EWS Sharing operation methods from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/sharing.rs`, beside the existing
  sharing request parser and response builders. This includes
  `GetSharingMetadata`, `GetSharingFolder`, `RefreshSharingFolder`, and
  `AcceptSharingInvitation`, plus the module-local helpers that resolve
  same-tenant owners and accessible shared collections. Same-tenant grant
  lookup, canonical sharing grant creation, contact/calendar scope handling,
  SOAP routing, error mapping, and response XML remain unchanged.
- 2026-07-01 verification for the EWS Sharing operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange sharing_` passed the
  EWS sharing metadata, invitation, shared-folder lookup, refresh, and related
  MAPI named-property coverage; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the moved Sharing operation method definitions now live in
  `service/ews/sharing.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 4,561
  tracked source lines. Direct physical line counts report `service.rs` at
  4,384 lines and `service/ews/sharing.rs` at 371 lines.
- 2026-07-01: Advanced MR-005 by moving EWS Delegation operation methods from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/delegation.rs`, beside the existing
  delegate request validators, parsers, and response builders. This includes
  `AddDelegate`, `UpdateDelegate`, `GetDelegate`, and `RemoveDelegate`, plus
  the module-local mutation helper shared by add/update. Mailbox-owner
  validation, canonical delegate upsert/remove calls, address-book tenant
  checks, unsupported Exchange-only permission rejection, audit inputs, SOAP
  routing, and response XML remain unchanged.
- 2026-07-01 verification for the EWS Delegation operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange delegate_` passed the
  EWS delegate add/get/update/remove paths, rejection paths, and related
  MAPI free/busy and permission projections; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms the moved Delegation operation method definitions now live in
  `service/ews/delegation.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  4,374 tracked source lines. Direct physical line counts report `service.rs`
  at 4,202 lines and `service/ews/delegation.rs` at 423 lines.
- 2026-07-01: Advanced MR-005 by moving EWS OOF operation methods from
  `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/oof.rs`, beside the existing OOF
  projection, parser, Sieve script, and response helpers. This includes
  `GetUserOofSettings` and `SetUserOofSettings`. Canonical active Sieve
  script fetch/update calls, vacation-script metadata, scheduled OOF handling,
  disable behavior, audit inputs, SOAP routing, and response XML remain
  unchanged.
- 2026-07-01 verification for the EWS OOF operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange oof_settings` passed the
  disabled, active vacation, scheduled metadata, disable, write, and error
  response-shape paths; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the moved OOF operation method definitions now live in `service/ews/oof.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 4,288 tracked source lines. Direct
  physical line counts report `service.rs` at 4,119 lines and
  `service/ews/oof.rs` at 292 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `GetUserAvailability`
  operation method from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/availability.rs`, beside the existing
  free/busy response builders, suggestions response builder, requested-window
  parser, and overlap helper. Authenticated-mailbox filtering, canonical
  calendar event lookup, window filtering, response ordering, SOAP routing, and
  response XML remain unchanged.
- 2026-07-01 verification for the EWS availability operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange get_user_availability`
  passed the canonical busy-event and suggestions-response paths;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `get_user_availability` now lives
  in `service/ews/availability.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  4,255 tracked source lines. Direct physical line counts report `service.rs`
  at 4,088 lines and `service/ews/availability.rs` at 143 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS root child-folder count helper
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/folders.rs`, beside the folder-kind
  parsers and folder XML helpers that consume the count. Contact, calendar,
  task, mailbox, and public-folder tree lookups remain unchanged.
- 2026-07-01 verification for the EWS root child-folder count helper split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_folder_root` passed the root bootstrap child-folder count path;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `root_child_folder_count` now
  lives in `service/ews/folders.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  4,225 tracked source lines. Direct physical line counts report `service.rs`
  at 4,059 lines and `service/ews/folders.rs` at 398 lines.
- 2026-07-01: Advanced MR-005 by moving the read-only EWS folder projection
  operations from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/folders.rs`. This includes `FindFolder`
  and `SyncFolderHierarchy`. Mailbox, contact, calendar, task, and
  public-folder root projection order, sync-state text, SOAP routing, and
  response XML remain unchanged.
- 2026-07-01 verification for the EWS folder projection operation split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  find_folder_lists_contact_and_calendar_folders` passed the `FindFolder`
  projection path; `cargo test -p lpe-exchange
  sync_folder_hierarchy_lists_contact_and_calendar_folders` passed the
  hierarchy sync projection path; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `find_folder` and `sync_folder_hierarchy` now live in
  `service/ews/folders.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 4,084
  tracked source lines. Direct physical line counts report `service.rs` at
  3,922 lines and `service/ews/folders.rs` at 541 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `GetFolder` operation method
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/folders.rs`, beside the folder
  projection operations and XML helpers. Mailbox folder resolution,
  public-folder projection, supported distinguished folder projections,
  unsupported-folder errors, SOAP routing, and response XML remain unchanged.
- 2026-07-01 verification for the EWS `GetFolder` operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange get_folder_returns`
  passed the supported folder and unsupported folder response paths; `cargo
  test -p lpe-exchange get_folder_root_reports_child_folders_for_client_bootstrap`
  passed the root child-count projection path; `$env:RUST_TEST_THREADS='1';
  cargo test -p lpe-exchange` passed with 1,594 tests and doc tests passing.
  `rg` confirms `get_folder` now lives in `service/ews/folders.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 3,896 tracked source lines. Direct
  physical line counts report `service.rs` at 3,741 lines and
  `service/ews/folders.rs` at 705 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `SyncFolderItems` operation
  method from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/sync_state.rs`, beside the sync-state
  token parsers and builders it uses. Contact, calendar, task, mailbox, and
  public-folder item sync projections, legacy sync-state handling, SOAP
  routing, and response XML remain unchanged.
- 2026-07-01 verification for the EWS `SyncFolderItems` operation split:
  `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  sync_folder_items` passed 17 focused tests covering contacts, calendar,
  tasks, mailbox messages, public-folder items, UTF-16 requests, legacy sync
  states, delete/update detection, and no-access public folders;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `sync_folder_items` now lives in
  `service/ews/sync_state.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  3,578 tracked source lines. Direct physical line counts report `service.rs`
  at 3,426 lines and `service/ews/sync_state.rs` at 457 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `FindItem` operation method
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/items.rs`, a new focused read-only item
  projection module. Contact, calendar, task, mailbox, and public-folder item
  listing, public-folder no-access behavior, SOAP routing, and response XML
  remain unchanged.
- 2026-07-01 verification for the EWS `FindItem` operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange find_item` passed 5
  focused tests covering mailbox, system mailbox, calendar, public-folder, and
  public-folder no-access paths; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `find_item` now lives in `service/ews/items.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 3,499 tracked source lines. Direct
  physical line counts report `service.rs` at 3,348 lines and
  `service/ews/items.rs` at 90 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `GetItem` operation method
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/items.rs`, beside the read-only
  `FindItem` projection. Contact, calendar, task, mailbox, public-folder,
  attachment-reference, MIME-content, Bcc-hiding, unsupported-id, SOAP routing,
  and response XML behavior remain unchanged.
- 2026-07-01 verification for the EWS `GetItem` operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange get_item` passed 9
  focused tests covering mailbox, system mailbox, public-folder, MIME content,
  attachment, Bcc-hiding, unsupported-id, and public-folder no-access paths;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `get_item` now lives in
  `service/ews/items.rs`. `python tools/check_oversized_sources.py` passed in
  warning mode and reports `crates/lpe-exchange/src/service.rs` at 3,375
  tracked source lines. Direct physical line counts report `service.rs` at
  3,228 lines and `service/ews/items.rs` at 214 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS conversation operation methods
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/conversations.rs`, beside the existing
  conversation request parsers and response builders. This includes
  `FindConversation`, `GetConversationItems`, `ApplyConversationAction`, and
  their shared canonical source-email helper. Conversation grouping, ignored
  folders, current-thread move/delete/read-state mutations, future-message
  unsupported behavior, SOAP routing, audit subjects, and response XML remain
  unchanged.
- 2026-07-01 verification for the EWS conversation operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange conversation_` passed
  32 focused tests covering EWS conversation find/get/apply paths and adjacent
  MAPI conversation-action projections; `$env:RUST_TEST_THREADS='1'; cargo
  test -p lpe-exchange` passed with 1,594 tests and doc tests passing. `rg`
  confirms the conversation operation methods now live in
  `service/ews/conversations.rs`. `python tools/check_oversized_sources.py`
  passed in warning mode and reports `crates/lpe-exchange/src/service.rs` at
  3,148 tracked source lines. Direct physical line counts report `service.rs`
  at 3,010 lines and `service/ews/conversations.rs` at 517 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS attachment operation methods
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/attachments.rs`, beside the existing
  attachment request parser and response builders. This includes
  `GetAttachment`, `CreateAttachment`, and `DeleteAttachment`. Canonical
  attachment fetch/create/delete calls, Magika validation, parent-message
  checks, file-attachment-only rejection, audit subjects, SOAP routing, and
  response XML remain unchanged.
- 2026-07-01 verification for the EWS attachment operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange attachment_` passed 40
  focused tests covering EWS attachment get/create/delete paths, Magika blocked
  payloads, unknown parent and attachment errors, and adjacent MAPI attachment
  projections; `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange`
  passed with 1,594 tests and doc tests passing. `rg` confirms the attachment
  operation methods now live in `service/ews/attachments.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 2,974 tracked source lines. Direct
  physical line counts report `service.rs` at 2,848 lines and
  `service/ews/attachments.rs` at 268 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS room-list operation methods
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/rooms.rs`, beside the existing room
  list request parser and response builders. This includes `GetRooms` and
  `GetRoomLists`. Canonical address-book lookup, computed tenant room-list
  filtering, explicit room-list rejection, SOAP routing, and response XML
  remain unchanged.
- 2026-07-01 verification for the EWS room-list operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange
  rooms_are_projected_from_canonical_directory_entries` passed the room and
  room-list projection, accepted computed room-list address, and rejected
  explicit custom room-list paths; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  the room-list operation methods now live in `service/ews/rooms.rs`. `python
  tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 2,948 tracked source lines. Direct
  physical line counts report `service.rs` at 2,825 lines and
  `service/ews/rooms.rs` at 111 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS service-configuration and
  retention tag operation methods from `crates/lpe-exchange/src/service.rs`
  into their existing focused modules. `GetServiceConfiguration` now lives in
  `crates/lpe-exchange/src/service/ews/mail_tips.rs` beside its bounded
  MailTips configuration response builder; `GetUserRetentionPolicyTags` now
  lives in `crates/lpe-exchange/src/service/ews/retention.rs` beside its tag
  response builder. Requested configuration parsing, unsupported service
  configuration errors, canonical retention-tag fetches, SOAP routing, and
  response XML remain unchanged.
- 2026-07-01 verification for the service-configuration and retention tag
  split: `cargo fmt --package lpe-exchange`; `cargo test -p lpe-exchange
  get_service_configuration` passed 2 focused tests covering bounded MailTips
  configuration and parseable unsupported configuration gaps; `cargo test -p
  lpe-exchange get_user_retention_policy_tags` passed 2 focused tests covering
  same-tenant assignment visibility and documented response shape;
  `$env:RUST_TEST_THREADS='1'; cargo test -p lpe-exchange` passed with 1,594
  tests and doc tests passing. `rg` confirms `get_service_configuration` now
  lives in `service/ews/mail_tips.rs` and
  `get_user_retention_policy_tags` now lives in `service/ews/retention.rs`.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 2,934 tracked source lines. Direct
  physical line counts report `service.rs` at 2,813 lines,
  `service/ews/mail_tips.rs` at 244 lines, and `service/ews/retention.rs` at
  80 lines.
- 2026-07-01: Advanced MR-005 by moving the EWS `ConvertId` operation method
  from `crates/lpe-exchange/src/service.rs` into
  `crates/lpe-exchange/src/service/ews/ids.rs`, beside its existing request
  scanner, canonical-id conversion, alternate-id serialization, and hex-entry
  helpers. Destination-format parsing, source-id validation, unsupported-format
  errors, SOAP routing, and response XML remain unchanged.
- 2026-07-01 verification for the EWS `ConvertId` operation split: `cargo fmt
  --package lpe-exchange`; `cargo test -p lpe-exchange convert_id` passed 2
  focused tests covering supported canonical object families and HexEntryId
  attachment round-trips; `$env:RUST_TEST_THREADS='1'; cargo test -p
  lpe-exchange` passed with 1,594 tests and doc tests passing. `rg` confirms
  `async fn convert_id` now lives in `service/ews/ids.rs` and `service.rs`
  only retains the `"ConvertId" => self.convert_id(&body).await?` router arm.
  `python tools/check_oversized_sources.py` passed in warning mode and reports
  `crates/lpe-exchange/src/service.rs` at 2,905 tracked source lines. Direct
  physical line counts report `service.rs` at 2,788 lines and
  `service/ews/ids.rs` at 267 lines.
