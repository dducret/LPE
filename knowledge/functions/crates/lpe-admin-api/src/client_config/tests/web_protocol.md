---
type: Rust Function
title: web_protocol
resource: crates/lpe-admin-api/src/client_config/tests.rs#L101-L106
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/client_config/tests/XmlNode/child_text
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape
---

# Signature

`fn web_protocol(account: &XmlNode) -> &XmlNode`

# Calls

- [child_text](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/XmlNode/child_text.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_autodiscover_web_external_uses_ms_oxdscli_protocol_shape.md)