---
type: Rust Function
title: append_postfix_style_mail_log
resource: LPE-CT/src/smtp/audit.rs#L94-L109
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_path
  - functions/tools/test_rca_outlook_trace_summary/FakePath/open
  - functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_line
  called_by:
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
---

# Signature

`fn append_postfix_style_mail_log(event: &TransportAuditEvent) -> Result<()>`

# Calls

- [postfix_style_mail_log_path](../../../../../functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_path.md)
- [open](../../../../../functions/tools/test_rca_outlook_trace_summary/FakePath/open.md)
- [postfix_style_mail_log_line](../../../../../functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_line.md)

# Called by

- [append_transport_audit](../../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)