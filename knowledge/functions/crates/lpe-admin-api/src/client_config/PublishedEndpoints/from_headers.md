---
type: Rust Method
title: from_headers
resource: crates/lpe-admin-api/src/client_config.rs#L209-L290
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/public_host
  - functions/crates/lpe-admin-api/src/client_config/host_without_port
  - functions/crates/lpe-admin-api/src/client_config/public_scheme
  - functions/crates/lpe-admin-api/src/client_config/read_u16_env
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/thunderbird_autoconfig
  - functions/crates/lpe-admin-api/src/client_config/jmap_well_known_location
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json
  - functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration
  - functions/crates/lpe-admin-api/src/client_config/tests/mapi_autodiscover_publication_is_env_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/invalid_mapi_http_capability_header_is_ignored
  - functions/crates/lpe-admin-api/src/client_config/tests/mapi_http_capability_header_and_enable_flag_publish_mapi
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins
  - functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_ews_publication_is_env_opt_in
  - functions/crates/lpe-admin-api/src/client_config/tests/soap_exchange_autodiscover_publication_is_env_opt_in
---

# Signature

`fn from_headers(headers: &HeaderMap, email_hint: Option<&str>) -> Self`

# Calls

- [public_host](../../../../../../functions/crates/lpe-admin-api/src/client_config/public_host.md)
- [host_without_port](../../../../../../functions/crates/lpe-admin-api/src/client_config/host_without_port.md)
- [public_scheme](../../../../../../functions/crates/lpe-admin-api/src/client_config/public_scheme.md)
- [read_u16_env](../../../../../../functions/crates/lpe-admin-api/src/client_config/read_u16_env.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [thunderbird_autoconfig](../../../../../../functions/crates/lpe-admin-api/src/client_config/thunderbird_autoconfig.md)
- [jmap_well_known_location](../../../../../../functions/crates/lpe-admin-api/src/client_config/jmap_well_known_location.md)
- [outlook_autodiscover_get](../../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_get.md)
- [outlook_autodiscover_post](../../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [outlook_autodiscover_json](../../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_json.md)
- [thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/thunderbird_autoconfig_publishes_imap_only_when_edge_imaps_is_configured.md)
- [outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_does_not_publish_imap_or_smtp_without_explicit_edge_configuration.md)
- [mapi_autodiscover_publication_is_env_opt_in](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_autodiscover_publication_is_env_opt_in.md)
- [invalid_mapi_http_capability_header_is_ignored](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/invalid_mapi_http_capability_header_is_ignored.md)
- [mapi_http_capability_header_and_enable_flag_publish_mapi](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/mapi_http_capability_header_and_enable_flag_publish_mapi.md)
- [legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_has_separate_provider_opt_ins.md)
- [legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_publication_works_with_ews_provider_opt_ins.md)
- [legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/legacy_exchange_autodiscover_survives_mapi_capability_header_without_mapi_publication.md)
- [outlook_autodiscover_ews_publication_is_env_opt_in](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_ews_publication_is_env_opt_in.md)
- [soap_exchange_autodiscover_publication_is_env_opt_in](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/soap_exchange_autodiscover_publication_is_env_opt_in.md)