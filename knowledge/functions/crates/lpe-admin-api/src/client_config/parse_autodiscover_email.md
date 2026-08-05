---
type: Rust Function
title: parse_autodiscover_email
resource: crates/lpe-admin-api/src/client_config.rs#L851-L857
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/xml_tag_value
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_email_address
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_namespaced_email_address
  - functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_soap_mailbox
---

# Signature

`fn parse_autodiscover_email(body: &[u8]) -> Option<String>`

# Calls

- [xml_tag_value](../../../../../functions/crates/lpe-admin-api/src/client_config/xml_tag_value.md)

# Called by

- [outlook_autodiscover_post](../../../../../functions/crates/lpe-admin-api/src/client_config/outlook_autodiscover_post.md)
- [autodiscover_request_parser_extracts_email_address](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_email_address.md)
- [autodiscover_request_parser_extracts_namespaced_email_address](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_namespaced_email_address.md)
- [autodiscover_request_parser_extracts_soap_mailbox](../../../../../functions/crates/lpe-admin-api/src/client_config/tests/autodiscover_request_parser_extracts_soap_mailbox.md)