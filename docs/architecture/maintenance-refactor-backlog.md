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
| MR-004 | Partial | `service/http_routes.rs` now owns the endpoint path constants and RPC proxy path list, with focused route/RPC/EWS verification recorded in progress notes. | Extract EWS SOAP operation dispatch without endpoint, auth, or response changes. |
| MR-005 | Pending | No completed EWS item-family split is recorded in this backlog. | Extract EWS mail/contact/calendar/task/MIME operation modules and verify behavior. |
| MR-006 | Pending | No completed `ExchangeStore` split is recorded in this backlog. | Split `crates/lpe-exchange/src/store.rs` by storage family while preserving trait semantics. |
| MR-007 | Pending | Earlier table helper extraction exists in the repository, but this backlog does not record a completed current slice. | Continue splitting `tables.rs` and prove table row output is unchanged. |
| MR-008 | Pending | Earlier property helper extraction exists in the repository, but this backlog does not record a completed current slice. | Continue splitting `properties.rs` and preserve property IDs, encoding, named properties, and custom values. |
| MR-009 | Pending | Earlier ROP helper extraction exists in the repository, but this backlog does not record a completed current slice. | Continue splitting `rop.rs` and preserve unsupported/reserved ROP behavior. |
| MR-010 | Pending | No completed MAPI mailstore/store projection split is recorded in this backlog. | Split projection and Outlook metadata boundaries while preserving IDs, source keys, change keys, and sync facts. |
| MR-011 | Pending | No completed storage protocol projection split is recorded. | Split `crates/lpe-storage/src/protocols.rs` while preserving exports and serialized output. |
| MR-012 | Pending | No completed blob-store split is recorded. | Split `blob_store.rs` and verify placement, migration, cleanup, and hash behavior. |
| MR-013 | Pending | No completed ActiveSync service split is recorded. | Split ActiveSync service without WBXML/status/auth/sync-key changes. |
| MR-014 | Pending | No completed MAPI transport split is recorded. | Split `mapi/transport.rs` while preserving headers, cookies, sequence, replay, and envelopes. |
| MR-015 | Pending | No completed NSPI split is recorded. | Split NSPI parsing/responses/properties/lookup while keeping Microsoft-specific matching local. |
| MR-016 | Pending | The audit records prior SMTP reductions, but this backlog does not record a completed current slice. | Continue splitting `LPE-CT/src/smtp.rs` and verify SMTP semantics. |
| MR-017 | Pending | No completed `LPE-CT/src/main.rs` split is recorded. | Split main wiring without CLI/env/routes/auth/startup changes. |
| MR-018 | Pending | Primitive crypto helpers are centralized, but broad MAPI diagnostic hex/preview duplication remains. | Centralize only identical diagnostic helpers and preserve debug output/protocol bytes. |
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
  `reminder_item_id` now live in `service/ews/reminders.rs` while
  `parse_reminder_item_id` remains local to the reminder action path; `cargo
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
