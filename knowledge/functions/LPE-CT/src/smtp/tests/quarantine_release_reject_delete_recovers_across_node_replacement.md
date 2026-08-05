---
type: Rust Function
title: quarantine_release_reject_delete_recovers_across_node_replacement
resource: LPE-CT/src/smtp/tests.rs#L2921-L2979
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/initialize_spool
  - functions/LPE-CT/src/smtp/tests/runtime_config
  - functions/LPE-CT/src/smtp/tests/inbound_test_message
  - functions/LPE-CT/src/smtp/queue_store/persist_message
  - functions/LPE-CT/src/smtp/trace_actions/load_trace_details
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn quarantine_release_reject_delete_recovers_across_node_replacement()`

# Calls

- [initialize_spool](../../../../../functions/LPE-CT/src/smtp/initialize_spool.md)
- [runtime_config](../../../../../functions/LPE-CT/src/smtp/tests/runtime_config.md)
- [inbound_test_message](../../../../../functions/LPE-CT/src/smtp/tests/inbound_test_message.md)
- [persist_message](../../../../../functions/LPE-CT/src/smtp/queue_store/persist_message.md)
- [load_trace_details](../../../../../functions/LPE-CT/src/smtp/trace_actions/load_trace_details.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)