---
type: Rust Function
title: postfix_style_mail_log_line
resource: LPE-CT/src/smtp/audit.rs#L139-L185
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/audit/sanitize_mail_log_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log
  - functions/LPE-CT/src/smtp/tests/postfix_style_mail_log_line_keeps_operator_correlation_fields
---

# Signature

`pub(in crate::smtp) fn postfix_style_mail_log_line(event: &TransportAuditEvent) -> String`

# Calls

- [sanitize_mail_log_value](../../../../../functions/LPE-CT/src/smtp/audit/sanitize_mail_log_value.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [append_postfix_style_mail_log](../../../../../functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log.md)
- [postfix_style_mail_log_line_keeps_operator_correlation_fields](../../../../../functions/LPE-CT/src/smtp/tests/postfix_style_mail_log_line_keeps_operator_correlation_fields.md)