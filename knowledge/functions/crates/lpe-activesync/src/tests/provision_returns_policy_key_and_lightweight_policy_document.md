---
type: Rust Function
title: provision_returns_policy_key_and_lightweight_policy_document
resource: crates/lpe-activesync/src/tests.rs#L1541-L1616
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/provision_request
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/tests/FakeStore/device_key
---

# Signature

`async fn provision_returns_policy_key_and_lightweight_policy_document()`

# Calls

- [provision_request](../../../../../functions/crates/lpe-activesync/src/tests/provision_request.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [device_key](../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/device_key.md)