---
type: Rust Module
title: tests
resource: LPE-CT/src/reporting/tests.rs#L1-L173
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-build-digest-report-default-reporting-settings-filter-quarantine-for-domain-filter-quarantine-for-mailbox-load-digest-report-normalize-reporting-settings-digestdomaindefault-digestuseroverride
  - external/crate-smtp-quarantinesummary
  - external/serde-json-value
  - external/std-fs-path-pathbuf-time-systemtime-unix-epoch
  member_of:
  - packages/LPE-CT
---

# Contains

- [temp_dir](../../../../functions/LPE-CT/src/reporting/tests/temp_dir.md)
- [sample_item](../../../../functions/LPE-CT/src/reporting/tests/sample_item.md)
- [reporting_defaults_are_normalized](../../../../functions/LPE-CT/src/reporting/tests/reporting_defaults_are_normalized.md)
- [reporting_normalization_deduplicates_domain_defaults_and_overrides](../../../../functions/LPE-CT/src/reporting/tests/reporting_normalization_deduplicates_domain_defaults_and_overrides.md)
- [domain_filter_matches_sender_and_recipient_domains](../../../../functions/LPE-CT/src/reporting/tests/domain_filter_matches_sender_and_recipient_domains.md)
- [mailbox_filter_matches_sender_and_recipient_mailboxes](../../../../functions/LPE-CT/src/reporting/tests/mailbox_filter_matches_sender_and_recipient_mailboxes.md)
- [digest_report_enriches_status_and_domain_counts_and_persists_artifact](../../../../functions/LPE-CT/src/reporting/tests/digest_report_enriches_status_and_domain_counts_and_persists_artifact.md)

# Imports

- `super::{
    build_digest_report, default_reporting_settings, filter_quarantine_for_domain,
    filter_quarantine_for_mailbox, load_digest_report, normalize_reporting_settings,
    DigestDomainDefault, DigestUserOverride,
}`
- `crate::smtp::QuarantineSummary`
- `serde_json::Value`
- `std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
}`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)