---
type: Rust Function
title: outbound_sender_policy_addresses
resource: LPE-CT/src/smtp.rs#L1208-L1221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy
---

# Signature

`fn outbound_sender_policy_addresses<'a>( payload: &'a OutboundMessageHandoffRequest, ) -> Vec<&'a str>`

# Calls

- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [evaluate_outbound_sender_policy](../../../../functions/LPE-CT/src/smtp/evaluate_outbound_sender_policy.md)