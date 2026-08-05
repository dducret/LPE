---
type: Rust Function
title: rop_backoff_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L136-L155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_logon_example
  - functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_targeted_rop_example
---

# Signature

`pub(in crate::mapi) fn rop_backoff_response( logon_id: u8, duration_ms: u32, backoff_rops: &[(u8, u32)], additional_data: &[u8], ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [backoff_response_matches_microsoft_logon_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_logon_example.md)
- [backoff_response_matches_microsoft_targeted_rop_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_targeted_rop_example.md)