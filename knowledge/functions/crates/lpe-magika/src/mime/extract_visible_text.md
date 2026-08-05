---
type: Rust Function
title: extract_visible_text
resource: crates/lpe-magika/src/mime.rs#L28-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_visible_part
  called_by:
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/trace/body_content
---

# Signature

`pub fn extract_visible_text(bytes: &[u8]) -> Result<String>`

# Calls

- [parse_visible_part](../../../../../functions/crates/lpe-magika/src/mime/parse_visible_part.md)

# Called by

- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [deliver_inbound_message](../../../../../functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message.md)
- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [body_content](../../../../../functions/LPE-CT/src/smtp/trace/body_content.md)