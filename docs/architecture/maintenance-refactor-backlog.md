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
| MR-004 | Pending | No completed service routing split is recorded in this backlog. | Extract EWS HTTP route/SOAP dispatch without endpoint or response changes. |
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
