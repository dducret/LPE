---
type: Rust Function
title: normalize_user_overrides
resource: LPE-CT/src/reporting.rs#L1250-L1270
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

`fn normalize_user_overrides(items: &[DigestUserOverride]) -> Vec<DigestUserOverride>`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [normalize_reporting_settings](../../../../functions/LPE-CT/src/reporting/normalize_reporting_settings.md)