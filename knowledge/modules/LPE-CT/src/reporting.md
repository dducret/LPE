---
type: Rust Module
title: reporting
resource: LPE-CT/src/reporting.rs#L1-L1444
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-context-result
  - external/axum-extract-query
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sqlx-row
  - external/std-collections-btreemap-btreeset-fs-path-path-pathbuf-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/crate-smtp-self-quarantinesummary-runtimeconfig-tracedetails
  member_of:
  - packages/LPE-CT
---

# Contains

- [ReportingSettings](../../../classes/LPE-CT/src/reporting/ReportingSettings.md)
- [DigestDomainDefault](../../../classes/LPE-CT/src/reporting/DigestDomainDefault.md)
- [DigestUserOverride](../../../classes/LPE-CT/src/reporting/DigestUserOverride.md)
- [HistoryQuery](../../../classes/LPE-CT/src/reporting/HistoryQuery.md)
- [MailHistorySummary](../../../classes/LPE-CT/src/reporting/MailHistorySummary.md)
- [MailHistoryResponse](../../../classes/LPE-CT/src/reporting/MailHistoryResponse.md)
- [MailHistoryEvent](../../../classes/LPE-CT/src/reporting/MailHistoryEvent.md)
- [TraceHistoryDetails](../../../classes/LPE-CT/src/reporting/TraceHistoryDetails.md)
- [DigestMetricCount](../../../classes/LPE-CT/src/reporting/DigestMetricCount.md)
- [DigestReportSummary](../../../classes/LPE-CT/src/reporting/DigestReportSummary.md)
- [DigestReportDetails](../../../classes/LPE-CT/src/reporting/DigestReportDetails.md)
- [ReportingSnapshot](../../../classes/LPE-CT/src/reporting/ReportingSnapshot.md)
- [DigestRunResponse](../../../classes/LPE-CT/src/reporting/DigestRunResponse.md)
- [StoredMailHistoryEvent](../../../classes/LPE-CT/src/reporting/StoredMailHistoryEvent.md)
- [from](../../../functions/LPE-CT/src/reporting/MailHistoryEvent/from-storedmailhistoryevent/from.md)
- [default_reporting_settings](../../../functions/LPE-CT/src/reporting/default_reporting_settings.md)
- [default_digest_interval_minutes](../../../functions/LPE-CT/src/reporting/default_digest_interval_minutes.md)
- [default_digest_max_items](../../../functions/LPE-CT/src/reporting/default_digest_max_items.md)
- [default_history_retention_days](../../../functions/LPE-CT/src/reporting/default_history_retention_days.md)
- [default_digest_report_retention_days](../../../functions/LPE-CT/src/reporting/default_digest_report_retention_days.md)
- [normalize_reporting_settings](../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [snapshot](../../../functions/LPE-CT/src/reporting/snapshot.md)
- [run_due_digest_generation](../../../functions/LPE-CT/src/reporting/run_due_digest_generation.md)
- [run_digest_generation](../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [list_recent_digest_reports](../../../functions/LPE-CT/src/reporting/list_recent_digest_reports.md)
- [load_digest_report](../../../functions/LPE-CT/src/reporting/load_digest_report.md)
- [search_mail_history](../../../functions/LPE-CT/src/reporting/search_mail_history.md)
- [load_trace_history](../../../functions/LPE-CT/src/reporting/load_trace_history.md)
- [read_mail_history_events](../../../functions/LPE-CT/src/reporting/read_mail_history_events.md)
- [read_mail_history_events_from_jsonl](../../../functions/LPE-CT/src/reporting/read_mail_history_events_from_jsonl.md)
- [enforce_retention](../../../functions/LPE-CT/src/reporting/enforce_retention.md)
- [search_mail_history_from_db](../../../functions/LPE-CT/src/reporting/search_mail_history_from_db.md)
- [load_trace_history_from_db](../../../functions/LPE-CT/src/reporting/load_trace_history_from_db.md)
- [mail_history_event_from_row](../../../functions/LPE-CT/src/reporting/mail_history_event_from_row.md)
- [history_cutoff](../../../functions/LPE-CT/src/reporting/history_cutoff.md)
- [group_history](../../../functions/LPE-CT/src/reporting/group_history.md)
- [summarize_trace_history](../../../functions/LPE-CT/src/reporting/summarize_trace_history.md)
- [history_matches](../../../functions/LPE-CT/src/reporting/history_matches.md)
- [build_digest_report](../../../functions/LPE-CT/src/reporting/build_digest_report.md)
- [render_digest_content](../../../functions/LPE-CT/src/reporting/render_digest_content.md)
- [filter_quarantine_for_domain](../../../functions/LPE-CT/src/reporting/filter_quarantine_for_domain.md)
- [filter_quarantine_for_mailbox](../../../functions/LPE-CT/src/reporting/filter_quarantine_for_mailbox.md)
- [policy_tags_from_event](../../../functions/LPE-CT/src/reporting/policy_tags_from_event.md)
- [default_bool_true](../../../functions/LPE-CT/src/reporting/default_bool_true.md)
- [normalize_domain_defaults](../../../functions/LPE-CT/src/reporting/normalize_domain_defaults.md)
- [normalize_user_overrides](../../../functions/LPE-CT/src/reporting/normalize_user_overrides.md)
- [ensure_digest_dir](../../../functions/LPE-CT/src/reporting/ensure_digest_dir.md)
- [digest_report_dir](../../../functions/LPE-CT/src/reporting/digest_report_dir.md)
- [digest_is_due](../../../functions/LPE-CT/src/reporting/digest_is_due.md)
- [normalized](../../../functions/LPE-CT/src/reporting/normalized.md)
- [domain_part](../../../functions/LPE-CT/src/reporting/domain_part.md)
- [parse_unix_timestamp](../../../functions/LPE-CT/src/reporting/parse_unix_timestamp.md)
- [current_unix_timestamp](../../../functions/LPE-CT/src/reporting/current_unix_timestamp.md)
- [current_timestamp](../../../functions/LPE-CT/src/reporting/current_timestamp.md)
- [timestamp_from_now](../../../functions/LPE-CT/src/reporting/timestamp_from_now.md)
- [latest_decision](../../../functions/LPE-CT/src/reporting/latest_decision.md)
- [summarize_digest_counts](../../../functions/LPE-CT/src/reporting/summarize_digest_counts.md)
- [render_metric_counts](../../../functions/LPE-CT/src/reporting/render_metric_counts.md)
- [enrich_digest_detail](../../../functions/LPE-CT/src/reporting/enrich_digest_detail.md)
- [prune_transport_audit_jsonl](../../../functions/LPE-CT/src/reporting/prune_transport_audit_jsonl.md)
- [prune_digest_reports](../../../functions/LPE-CT/src/reporting/prune_digest_reports.md)
- [prune_retained_rows_from_db](../../../functions/LPE-CT/src/reporting/prune_retained_rows_from_db.md)

# Imports

- `anyhow::{Context, Result}`
- `axum::extract::Query`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sqlx::Row`
- `std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
}`
- `uuid::Uuid`
- `crate::smtp::{self, QuarantineSummary, RuntimeConfig, TraceDetails}`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)