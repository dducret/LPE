---
type: Rust Function
title: render_ews_web_autodiscover_protocol
resource: crates/lpe-admin-api/src/client_config.rs#L642-L664
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/web_external_protocol
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover
---

# Signature

`fn render_ews_web_autodiscover_protocol(config: &PublishedEndpoints, email: &str) -> String`

# Calls

- [web_external_protocol](../../../../../functions/crates/lpe-admin-api/src/client_config/web_external_protocol.md)

# Called by

- [render_outlook_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_outlook_autodiscover.md)