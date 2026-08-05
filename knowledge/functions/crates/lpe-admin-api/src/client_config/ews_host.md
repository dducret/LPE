---
type: Rust Function
title: ews_host
resource: crates/lpe-admin-api/src/client_config.rs#L1178-L1185
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/mailbox_server_name
  - functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover
---

# Signature

`fn ews_host(ews_url: &str) -> Option<&str>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [mailbox_server_name](../../../../../functions/crates/lpe-admin-api/src/client_config/mailbox_server_name.md)
- [render_soap_user_settings_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover.md)