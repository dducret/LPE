---
type: Rust Function
title: timestamp_from_now
resource: LPE-CT/src/reporting.rs#L1315-L1317
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/reporting/default_reporting_settings
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
  - functions/LPE-CT/src/reporting/run_digest_generation
---

# Signature

`fn timestamp_from_now(seconds: u64) -> String`

# Called by

- [default_reporting_settings](../../../../functions/LPE-CT/src/reporting/default_reporting_settings.md)
- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)
- [run_digest_generation](../../../../functions/LPE-CT/src/reporting/run_digest_generation.md)