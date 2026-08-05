---
type: Rust Function
title: render_mobilesync_autodiscover
resource: crates/lpe-admin-api/src/client_config.rs#L687-L715
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/tests/mobilesync_autodiscover_publishes_activesync_endpoint
---

# Signature

`fn render_mobilesync_autodiscover(config: &PublishedEndpoints, email: Option<&str>) -> String`

# Called by

- [outlook_autodiscover_post](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [mobilesync_autodiscover_publishes_activesync_endpoint](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mobilesync_autodiscover_publishes_activesync_endpoint.md)