---
type: Rust Function
title: render_soap_user_settings_autodiscover
resource: crates/lpe-admin-api/src/client_config.rs#L717-L803
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/ews_host
  - functions/crates/lpe-admin-api/src/client_config/fallback_host
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_response
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_publishes_ews_user_settings_when_enabled
---

# Signature

`fn render_soap_user_settings_autodiscover( config: &PublishedEndpoints, email: Option<&str>, ) -> String`

# Calls

- [ews_host](../../../../../functions/crates/lpe-admin-api/src/client_config/ews_host.md)
- [fallback_host](../../../../../functions/crates/lpe-admin-api/src/client_config/fallback_host.md)

# Called by

- [render_soap_user_settings_response](../../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_response.md)
- [soap_autodiscover_publishes_ews_user_settings_when_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_publishes_ews_user_settings_when_enabled.md)