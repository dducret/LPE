---
type: Rust Function
title: append_transport_audit
resource: LPE-CT/src/smtp/audit.rs#L33-L92
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/mime/parse_rfc822_header_value
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log
  - functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event
  called_by:
  - functions/LPE-CT/src/smtp/process_outbound_handoff
  - functions/LPE-CT/src/smtp/session/receive_message_with_validator
  - functions/LPE-CT/src/smtp/trace_actions/transition_trace
---

# Signature

`pub(in crate::smtp) async fn append_transport_audit( spool_dir: &Path, config: &RuntimeConfig, queue: &str, message: &QueuedMessage, ) -> Result<()>`

# Calls

- [parse_rfc822_header_value](../../../../../functions/crates/lpe-magika/src/mime/parse_rfc822_header_value.md)
- [open](../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [append_postfix_style_mail_log](../../../../../functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log.md)
- [persist_transport_audit_db_event](../../../../../functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event.md)

# Called by

- [process_outbound_handoff](../../../../../functions/LPE-CT/src/smtp/process_outbound_handoff.md)
- [receive_message_with_validator](../../../../../functions/LPE-CT/src/smtp/session/receive_message_with_validator.md)
- [transition_trace](../../../../../functions/LPE-CT/src/smtp/trace_actions/transition_trace.md)