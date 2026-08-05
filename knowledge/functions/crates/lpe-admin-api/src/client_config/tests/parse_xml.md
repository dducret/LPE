---
type: Rust Function
title: parse_xml
resource: crates/lpe-admin-api/src/client_config/tests.rs#L38-L87
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-admin-api/src/client_config/tests/outlook_account
---

# Signature

`fn parse_xml(xml: &str) -> XmlNode`

# Calls

- [from_str](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [outlook_account](../../../../../../functions/crates/lpe-admin-api/src/client_config/tests/outlook_account.md)