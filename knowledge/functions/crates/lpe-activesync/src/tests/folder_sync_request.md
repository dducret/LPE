---
type: Rust Function
title: folder_sync_request
resource: crates/lpe-activesync/src/tests.rs#L1798-L1804
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds
  - functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably
  - functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version
  - functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key
  - functions/crates/lpe-activesync/src/tests/permissive_mode_preserves_current_unprovisioned_behavior
---

# Signature

`fn folder_sync_request(sync_key: &str) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [post_with_supported_protocol_version_succeeds](../../../../../functions/crates/lpe-activesync/src/tests/post_with_supported_protocol_version_succeeds.md)
- [post_with_unsupported_protocol_version_is_rejected_predictably](../../../../../functions/crates/lpe-activesync/src/tests/post_with_unsupported_protocol_version_is_rejected_predictably.md)
- [unsupported_protocol_version_response_does_not_echo_request_version](../../../../../functions/crates/lpe-activesync/src/tests/unsupported_protocol_version_response_does_not_echo_request_version.md)
- [enforced_mode_validates_later_command_policy_key](../../../../../functions/crates/lpe-activesync/src/tests/enforced_mode_validates_later_command_policy_key.md)
- [permissive_mode_preserves_current_unprovisioned_behavior](../../../../../functions/crates/lpe-activesync/src/tests/permissive_mode_preserves_current_unprovisioned_behavior.md)