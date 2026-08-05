---
type: Rust Function
title: fallback_host
resource: crates/lpe-admin-api/src/client_config.rs#L602-L607
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/mailbox_server_name
  - functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover
---

# Signature

`fn fallback_host(config: &PublishedEndpoints) -> &str`

# Called by

- [mailbox_server_name](../../../../../functions/crates/lpe-admin-api/src/client_config/mailbox_server_name.md)
- [render_soap_user_settings_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover.md)