---
type: Rust Module
title: outlook_startup
resource: crates/lpe-exchange/src/mapi/outlook_startup.rs#L1-L421
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-session-mapisession
  - external/super-sync-inbox-folder-id-ipm-subtree-folder-id
  - external/super
  - external/crate-mapi-sync-calendar-folder-id
  - external/super-super-session-mapilogonidentitydebug
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [OutlookStartupGateSummary](../../../../../classes/crates/lpe-exchange/src/mapi/outlook_startup/OutlookStartupGateSummary.md)
- [normalized_rop_sequence_signature](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_rop_sequence_signature.md)
- [push_compressed_rop](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/push_compressed_rop.md)
- [outlook_startup_gate_summary](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/outlook_startup_gate_summary.md)
- [normal_inbox_visible_row_missing_reason](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_missing_reason.md)
- [normal_inbox_visible_row_release_request_shape](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normal_inbox_visible_row_release_request_shape.md)
- [configured_smart_input_variant](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/configured_smart_input_variant.md)
- [normalized_signature_collapses_repeated_release_loops](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/normalized_signature_collapses_repeated_release_loops.md)
- [classifier_reports_first_missing_gate_after_fai_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_first_missing_gate_after_fai_query_rows.md)
- [classifier_requires_inbox_query_rows_after_inbox_table_open](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_requires_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_query_rows_after_inbox_table_open](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_query_rows_after_inbox_table_open.md)
- [classifier_accepts_inbox_find_row_after_inbox_table_open](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_inbox_find_row_after_inbox_table_open.md)
- [classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_inbox_content_ics_as_normal_contents_table_progress.md)
- [classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_does_not_treat_non_inbox_content_ics_as_inbox_progress.md)
- [classifier_reports_visible_inbox_release_before_query_rows](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_visible_inbox_release_before_query_rows.md)
- [classifier_accepts_exact_ipm_configuration_findrow_gate](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_exact_ipm_configuration_findrow_gate.md)
- [classifier_accepts_findrow_delivered_fai_content](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_accepts_findrow_delivered_fai_content.md)
- [classifier_reports_inbox_contents_gate_after_receive_folder_verified](../../../../../functions/crates/lpe-exchange/src/mapi/outlook_startup/classifier_reports_inbox_contents_gate_after_receive_folder_verified.md)

# Imports

- `super::session::MapiSession`
- `super::sync::{INBOX_FOLDER_ID, IPM_SUBTREE_FOLDER_ID}`
- `super::*`
- `crate::mapi::sync::CALENDAR_FOLDER_ID`
- `super::super::session::MapiLogonIdentityDebug`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)