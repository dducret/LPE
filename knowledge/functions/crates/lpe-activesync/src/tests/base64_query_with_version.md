---
type: Rust Function
title: base64_query_with_version
resource: crates/lpe-activesync/src/tests.rs#L1893-L1917
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/tests/base64_query
  - functions/crates/lpe-activesync/src/tests/base64_query_rejects_unsupported_protocol_version
---

# Signature

`fn base64_query_with_version( protocol_version: u8, command_code: u8, device_id: &str, params: &[(u8, &[u8])], ) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [base64_query](../../../../../functions/crates/lpe-activesync/src/tests/base64_query.md)
- [base64_query_rejects_unsupported_protocol_version](../../../../../functions/crates/lpe-activesync/src/tests/base64_query_rejects_unsupported_protocol_version.md)