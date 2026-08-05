---
type: Rust Function
title: normalize_policy_settings
resource: LPE-CT/src/dashboard_config.rs#L266-L323
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/dashboard_config/default_antivirus_provider_chain
  - functions/LPE-CT/src/dashboard_config/normalize_csv_rules
  - functions/LPE-CT/src/dashboard_config/normalize_attachment_extension_rules
  - functions/LPE-CT/src/dashboard_config/default_dkim_headers
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/http_routes/update_policies
  - functions/LPE-CT/src/main
---

# Signature

`pub(crate) fn normalize_policy_settings(policies: &mut PolicySettings)`

# Calls

- [default_antivirus_provider_chain](../../../../functions/LPE-CT/src/dashboard_config/default_antivirus_provider_chain.md)
- [normalize_csv_rules](../../../../functions/LPE-CT/src/dashboard_config/normalize_csv_rules.md)
- [normalize_attachment_extension_rules](../../../../functions/LPE-CT/src/dashboard_config/normalize_attachment_extension_rules.md)
- [default_dkim_headers](../../../../functions/LPE-CT/src/dashboard_config/default_dkim_headers.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [update_policies](../../../../functions/LPE-CT/src/http_routes/update_policies.md)
- [main](../../../../functions/LPE-CT/src/main.md)