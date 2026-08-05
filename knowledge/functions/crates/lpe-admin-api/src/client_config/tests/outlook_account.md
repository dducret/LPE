---
type: Rust Function
title: outlook_account
resource: crates/lpe-admin-api/src/client_config/tests.rs#L93-L99
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/tests/parse_xml
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape
---

# Signature

`fn outlook_account(xml: &str) -> XmlNode`

# Calls

- [parse_xml](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/parse_xml.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape.md)