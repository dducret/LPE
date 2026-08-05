---
type: Rust Function
title: normalize_domain_defaults
resource: LPE-CT/src/reporting.rs#L1227-L1248
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/reporting/normalize_reporting_settings
---

# Signature

`fn normalize_domain_defaults(items: &[DigestDomainDefault]) -> Vec<DigestDomainDefault>`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)