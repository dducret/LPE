---
type: Rust Function
title: base64_query
resource: crates/lpe-activesync/src/tests.rs#L1889-L1891
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/base64_query_with_version
  called_by:
  - functions/crates/lpe-activesync/src/tests/parsed_base64_query
  - functions/crates/lpe-activesync/src/tests/base64_query_decodes_ashttp_fields
---

# Signature

`fn base64_query(command_code: u8, device_id: &str, params: &[(u8, &[u8])]) -> String`

# Calls

- [base64_query_with_version](../../../../../functions/crates/lpe-activesync/src/tests/base64_query_with_version.md)

# Called by

- [parsed_base64_query](../../../../../functions/crates/lpe-activesync/src/tests/parsed_base64_query.md)
- [base64_query_decodes_ashttp_fields](../../../../../functions/crates/lpe-activesync/src/tests/base64_query_decodes_ashttp_fields.md)