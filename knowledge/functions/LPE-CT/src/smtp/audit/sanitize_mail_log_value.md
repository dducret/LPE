---
type: Rust Function
title: sanitize_mail_log_value
resource: LPE-CT/src/smtp/audit.rs#L187-L201
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_line
---

# Signature

`fn sanitize_mail_log_value(value: &str) -> String`

# Called by

- [postfix_style_mail_log_line](../../../../../functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_line.md)