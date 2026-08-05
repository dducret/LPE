---
type: Rust Function
title: provision_acknowledgement_stores_active_policy_key
resource: crates/lpe-activesync/src/tests.rs#L1618-L1667
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/active_sync_query
  - functions/crates/lpe-activesync/src/tests/provision_request
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/tests/FakeStore/device_key
---

# Signature

`async fn provision_acknowledgement_stores_active_policy_key()`

# Calls

- [active_sync_query](../../../../../functions/crates/lpe-activesync/src/tests/active_sync_query.md)
- [provision_request](../../../../../functions/crates/lpe-activesync/src/tests/provision_request.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [device_key](../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)