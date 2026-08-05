---
type: Rust Function
title: render_soap_user_settings_response
resource: crates/lpe-admin-api/src/client_config.rs#L805-L812
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/soap_exchange_autodiscover_enabled
  - functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_is_not_published_for_default_outlook_imap_profile
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_exchange_autodiscover_publication_is_env_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_reports_mapi_http_enabled_when_opted_in
---

# Signature

`fn render_soap_user_settings_response( config: &PublishedEndpoints, email: Option<&str>, ) -> Option<String>`

# Calls

- [soap_exchange_autodiscover_enabled](../../../../../functions/crates/lpe-admin-api/src/client_config/PublishedEndpoints/soap_exchange_autodiscover_enabled.md)
- [render_soap_user_settings_autodiscover](../../../../../functions/crates/lpe-admin-api/src/client_config/render_soap_user_settings_autodiscover.md)

# Called by

- [outlook_autodiscover_post](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [soap_autodiscover_is_not_published_for_default_outlook_imap_profile](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_is_not_published_for_default_outlook_imap_profile.md)
- [soap_exchange_autodiscover_publication_is_env_opt_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_exchange_autodiscover_publication_is_env_opt_in.md)
- [soap_autodiscover_reports_mapi_http_enabled_when_opted_in](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_autodiscover_reports_mapi_http_enabled_when_opted_in.md)