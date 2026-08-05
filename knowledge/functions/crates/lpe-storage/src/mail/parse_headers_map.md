---
type: Rust Function
title: parse_headers_map
resource: crates/lpe-storage/src/mail.rs#L120-L124
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message
---

# Signature

`pub fn parse_headers_map(raw_message: &[u8]) -> HashMap<String, String>`

# Called by

- [deliver_inbound_message](../../../../../functions/crates/lpe-storage/src/inbound/Storage/deliver_inbound_message.md)