---
type: Rust Module
title: helpers
resource: crates/lpe-storage/src/admin/helpers.rs#L1-L298
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-core-sieve-parse-script-action-matchtype-statement-test
  - external/sqlx-row
  - external/crate-emailtraceresult-emailtracerow-mailflowentry-mailflowrow
  - external/super-map-email-trace-row-map-mail-flow-row
  - external/crate-emailtracerow-mailflowrow
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [map_mail_flow_row](../../../../../functions/crates/lpe-storage/src/admin/helpers/map_mail_flow_row.md)
- [map_email_trace_row](../../../../../functions/crates/lpe-storage/src/admin/helpers/map_email_trace_row.md)
- [mailbox_rule_summaries](../../../../../functions/crates/lpe-storage/src/admin/helpers/mailbox_rule_summaries.md)
- [summarize_statements_conditions](../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_conditions.md)
- [collect_statement_conditions](../../../../../functions/crates/lpe-storage/src/admin/helpers/collect_statement_conditions.md)
- [summarize_test](../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_test.md)
- [summarize_match_type](../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_match_type.md)
- [summarize_statements_actions](../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_statements_actions.md)
- [collect_statement_actions](../../../../../functions/crates/lpe-storage/src/admin/helpers/collect_statement_actions.md)
- [summarize_action](../../../../../functions/crates/lpe-storage/src/admin/helpers/summarize_action.md)
- [unsupported_exchange_rule_features](../../../../../functions/crates/lpe-storage/src/admin/helpers/unsupported_exchange_rule_features.md)
- [unsupported_client_local_profile_state](../../../../../functions/crates/lpe-storage/src/admin/helpers/unsupported_client_local_profile_state.md)
- [count_from_row](../../../../../functions/crates/lpe-storage/src/admin/helpers/count_from_row.md)
- [mail_flow_mapping_keeps_explicit_submission_and_sent_signals](../../../../../functions/crates/lpe-storage/src/admin/helpers/mail_flow_mapping_keeps_explicit_submission_and_sent_signals.md)
- [email_trace_mapping_surfaces_latest_queue_state](../../../../../functions/crates/lpe-storage/src/admin/helpers/email_trace_mapping_surfaces_latest_queue_state.md)

# Imports

- `anyhow::Result`
- `lpe_core::sieve::{parse_script, Action, MatchType, Statement, Test}`
- `sqlx::Row`
- `crate::{EmailTraceResult, EmailTraceRow, MailFlowEntry, MailFlowRow}`
- `super::{map_email_trace_row, map_mail_flow_row}`
- `crate::{EmailTraceRow, MailFlowRow}`
- `uuid::Uuid`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)