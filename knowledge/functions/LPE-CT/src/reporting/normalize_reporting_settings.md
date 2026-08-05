---
type: Rust Function
title: normalize_reporting_settings
resource: LPE-CT/src/reporting.rs#L293-L315
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/reporting/default_digest_interval_minutes
  - functions/LPE-CT/src/reporting/default_digest_max_items
  - functions/LPE-CT/src/reporting/default_history_retention_days
  - functions/LPE-CT/src/reporting/default_digest_report_retention_days
  - functions/LPE-CT/src/reporting/normalize_domain_defaults
  - functions/LPE-CT/src/reporting/normalize_user_overrides
  - functions/LPE-CT/src/reporting/timestamp_from_now
  called_by:
  - functions/LPE-CT/src/http_routes/update_reporting
  - functions/LPE-CT/src/main
  - functions/LPE-CT/src/reporting/run_due_digest_generation
  - functions/LPE-CT/src/reporting/run_digest_generation
  - functions/LPE-CT/src/reporting/tests/reporting_defaults_are_normalized
  - functions/LPE-CT/src/reporting/tests/reporting_normalization_deduplicates_domain_defaults_and_overrides
---

# Signature

`pub(crate) fn normalize_reporting_settings(settings: &mut ReportingSettings)`

# Calls

- [default_digest_interval_minutes](../../../../functions/LPE-CT/src/reporting/default_digest_interval_minutes.md)
- [default_digest_max_items](../../../../functions/LPE-CT/src/reporting/default_digest_max_items.md)
- [default_history_retention_days](../../../../functions/LPE-CT/src/reporting/default_history_retention_days.md)
- [default_digest_report_retention_days](../../../../functions/LPE-CT/src/reporting/default_digest_report_retention_days.md)
- [normalize_domain_defaults](../../../../functions/LPE-CT/src/reporting/normalize_domain_defaults.md)
- [normalize_user_overrides](../../../../functions/LPE-CT/src/reporting/normalize_user_overrides.md)
- [timestamp_from_now](../../../../functions/LPE-CT/src/reporting/timestamp_from_now.md)

# Called by

- [update_reporting](../../../../functions/LPE-CT/src/http_routes/update_reporting.md)
- [main](../../../../functions/LPE-CT/src/main.md)
- [run_due_digest_generation](../../../../functions/LPE-CT/src/reporting/run_due_digest_generation.md)
- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)
- [reporting_defaults_are_normalized](../../../../functions/LPE-CT/src/reporting/tests/reporting_defaults_are_normalized.md)
- [reporting_normalization_deduplicates_domain_defaults_and_overrides](../../../../functions/LPE-CT/src/reporting/tests/reporting_normalization_deduplicates_domain_defaults_and_overrides.md)