---
type: Rust Function
title: direct_mx_failure
resource: LPE-CT/src/smtp/dsn.rs#L87-L143
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/retry_after_seconds
  - functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status
  called_by:
  - functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx
---

# Signature

`pub(super) fn direct_mx_failure( route: &TransportRouteDecision, attempt_count: u32, detail: String, remote_host: Option<String>, permanent: bool, ) -> OutboundExecution`

# Calls

- [retry_after_seconds](../../../../../functions/LPE-CT/src/smtp/retry_after_seconds.md)
- [default_queue_for_status](../../../../../functions/LPE-CT/src/smtp/outbound_policy/default_queue_for_status.md)

# Called by

- [relay_message_direct_mx](../../../../../functions/LPE-CT/src/smtp/outbound_delivery/relay_message_direct_mx.md)