---
type: Rust Function
title: render_thunderbird_autoconfig
resource: crates/lpe-admin-api/src/client_config.rs#L389-L445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/thunderbird_autoconfig
  - functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured
  - functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_can_publish_explicit_submission_endpoint
---

# Signature

`fn render_thunderbird_autoconfig(config: &PublishedEndpoints) -> String`

# Called by

- [thunderbird_autoconfig](../../../../../functions/crates/lpe-admin-api/src/client_config/thunderbird_autoconfig.md)
- [thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured.md)
- [thunderbird_autoconfig_can_publish_explicit_submission_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_can_publish_explicit_submission_endpoint.md)