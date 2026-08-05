---
type: Rust Function
title: parse_rfc822_header_value
resource: crates/lpe-magika/src/mime.rs#L19-L26
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes
  - functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/outlook_test_message/classify_smtp_message
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
  - functions/LPE-CT/src/smtp/audit/quarantine_search_text
  - functions/LPE-CT/src/smtp/bayes/train_bayespam
  - functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message
  - functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy
  - functions/LPE-CT/src/smtp/quarantine/quarantine_metadata
  - functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message
  - functions/LPE-CT/src/smtp/trace/trace_details_from_message
---

# Signature

`pub fn parse_rfc822_header_value(bytes: &[u8], name: &str) -> Option<String>`

# Calls

- [split_headers_and_body_bytes](../../../../../functions/crates/lpe-magika/src/mime/split_headers_and_body_bytes.md)
- [parse_rfc822_headers_bytes](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_headers_bytes.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [classify_smtp_message](../../../../../functions/LPE-CT/src/outlook_test_message/classify_smtp_message.md)
- [append_transport_audit](../../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [quarantine_search_text](../../../../../functions/LPE-CT/src/smtp/audit/quarantine_search_text.md)
- [train_bayespam](../../../../../functions/LPE-CT/src/smtp/bayes/train_bayespam.md)
- [deliver_inbound_message](../../../../../functions/LPE-CT/src/smtp/delivery_bridge/deliver_inbound_message.md)
- [evaluate_inbound_policy](../../../../../functions/LPE-CT/src/smtp/inbound_policy/evaluate_inbound_policy.md)
- [quarantine_metadata](../../../../../functions/LPE-CT/src/smtp/quarantine/quarantine_metadata.md)
- [quarantine_summary_from_message](../../../../../functions/LPE-CT/src/smtp/trace/quarantine_summary_from_message.md)
- [trace_details_from_message](../../../../../functions/LPE-CT/src/smtp/trace/trace_details_from_message.md)