---
type: Rust Function
title: provision_request
resource: crates/lpe-activesync/src/tests.rs#L1772-L1795
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document
  - functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
---

# Signature

`fn provision_request(policy_key: Option<&str>, status: Option<&str>) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [provision_returns_policy_key_and_lightweight_policy_document](../../../../../functions/crates/lpe-activesync/src/tests/provision_returns_policy_key_and_lightweight_policy_document.md)
- [provision_acknowledgement_stores_active_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/provision_acknowledgement_stores_active_policy_key.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)