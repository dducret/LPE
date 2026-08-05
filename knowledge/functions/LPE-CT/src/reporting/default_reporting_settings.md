---
type: Rust Function
title: default_reporting_settings
resource: LPE-CT/src/reporting.rs#L261-L275
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/default_digest_interval_minutes
  - functions/LPE-CT/src/reporting/default_digest_max_items
  - functions/LPE-CT/src/reporting/default_history_retention_days
  - functions/LPE-CT/src/reporting/default_digest_report_retention_days
  - functions/LPE-CT/src/reporting/timestamp_from_now
  called_by:
  - functions/LPE-CT/src/dashboard_config/default_state
  - functions/LPE-CT/src/reporting/tests/reporting_defaults_are_normalized
  - functions/LPE-CT/src/reporting/tests/reporting_normalization_deduplicates_domain_defaults_and_overrides
---

# Signature

`pub(crate) fn default_reporting_settings() -> ReportingSettings`

# Calls

- [default_digest_interval_minutes](../../../../functions/LPE-CT/src/reporting/default_digest_interval_minutes.md)
- [default_digest_max_items](../../../../functions/LPE-CT/src/reporting/default_digest_max_items.md)
- [default_history_retention_days](../../../../functions/LPE-CT/src/reporting/default_history_retention_days.md)
- [default_digest_report_retention_days](../../../../functions/LPE-CT/src/reporting/default_digest_report_retention_days.md)
- [timestamp_from_now](../../../../functions/LPE-CT/src/reporting/timestamp_from_now.md)

# Called by

- [default_state](../../../../functions/LPE-CT/src/dashboard_config/default_state.md)
- [reporting_defaults_are_normalized](../../../../functions/LPE-CT/src/reporting/tests/reporting_defaults_are_normalized.md)
- [reporting_normalization_deduplicates_domain_defaults_and_overrides](../../../../functions/LPE-CT/src/reporting/tests/reporting_normalization_deduplicates_domain_defaults_and_overrides.md)