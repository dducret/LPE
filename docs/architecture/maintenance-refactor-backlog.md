# Maintenance Status

This document is the current maintenance view for the 0.5.x baseline. Retired
implementation history remains available through Git history only.

## Completed baseline work

- MAPI dispatch, tables, properties, ROP parsing, transport, NSPI, mailstore,
  and diagnostics were split into focused modules below the production source
  threshold where practical.
- EWS routing and operation families were split without introducing protocol
  local canonical state.
- ActiveSync service families were split while preserving canonical storage
  mutations.
- LPE-CT SMTP, management routes, dashboard configuration, browser code, CSS,
  and localization data were split into focused modules.
- Outlook RCA tooling was split into focused HTTP, EWS, MAPI, and CLI helpers.
- The canonical SQL schema remains a deliberate dense-file exception because
  installation and runtime drift validation consume it as one SQL program.
- The 0.5.1 updater accepts only the current `0.5.1-sql` schema or the exact
  late canonical physical form of the `0.5.0-sql-v1` source, rejects older
  same-label forms through a read-only preflight, and writes the target label
  only after physical target-shape validation.

## Active maintenance rules

- Preserve canonical mailbox, collaboration, rights, submission, recovery,
  and user-visible state outside protocol adapters.
- Preserve exact MAPI wire bytes, property IDs, named-property mappings,
  source/change keys, ROP boundaries, and unsupported error behavior when
  reorganizing Outlook-critical code.
- Keep `lib.rs`, `services.rs`, and `mapi.rs` as wiring hubs.
- Keep production source files below 1,500 lines except documented dense data
  or protocol tables.
- Run `python3 tools/check_repository.py` before review.
- Run the focused protocol crate tests and then the full affected crate suite
  after behavior changes.

## Active follow-up work

| Area | Current objective | Verification |
| --- | --- | --- |
| Outlook 0.5.1 acceptance | Re-run Calendar, Contacts, Tasks, offline reopen, send/receive, read state, notifications, and restart against a fresh `0.5.1-sql` database and clean Outlook profile. | Real Outlook trace, no crash/dialog, canonical state visible across protocols. |
| Public MAPI publication | Keep `/mapi/emsmdb` and `/mapi/nspi` authenticated and exposed through LPE-CT; keep legacy `EXPR` independently gated. | `cargo test -p lpe-exchange`, `cargo test -p lpe-admin-api`, `check-lpe.sh`, RCA readiness. |
| Outlook notification latency | Continue measuring delivery and folder-counter refresh latency without weakening durable replay. | Notification/table tests plus real-client timing logs. |
| MAPI storage metadata | Before adding further behavior to the 1,000-line `store/storage_impl/mapi_metadata.rs` helper, split it along the existing trait responsibilities: identities; named properties and custom values; profile properties and synchronization checkpoints. Keep `store/implementation.rs` as wiring only and preserve each SQL transaction boundary. | Focused PostgreSQL MAPI identity, named-property, profile, and synchronization tests, then `cargo test -p lpe-exchange`. |
| Public folders | Complete only the parity gaps required by declared public-folder support; do not block private mailbox MAPI publication on optional cross-server replication. | Public-folder MAPI/EWS tests and explicit client evidence. |
| Storage providers | Continue the provider-specific storage roadmap only after the current PostgreSQL/S3-compatible integrity gates remain green. | Storage migration, cleanup, rollback, quota, and restore tests. |
| Installation | Keep fresh-database initialization deterministic and keep in-place updates limited to the preflighted late physical form of the explicitly reviewed `0.5.0-sql-v1` to `0.5.1-sql` transition. | Shell syntax, isolated schema drift test, `check-lpe.sh`. |
| Oversized modules | Split the remaining over-threshold MAPI dispatch/transport modules, `lpe-storage` collaboration module, and Outlook RCA tooling by existing responsibility boundaries. Keep the canonical SQL schema as the documented dense-file exception. | `python3 tools/check_repository.py --fail` after each bounded split plus the affected crate/tool tests. |

## Deferred architecture decisions

- Cross-process MAPI session replay and load-balanced failover.
- Full Outlook Anywhere / RPC over HTTP publication through legacy `EXPR`.
- Provider-specific AWS S3 and Azure Blob behavior beyond the current
  provider-neutral storage contract.
- First-class thread lifecycle storage when JMAP or MAPI conversation behavior
  requires it.
