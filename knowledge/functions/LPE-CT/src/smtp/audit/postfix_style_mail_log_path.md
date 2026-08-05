---
type: Rust Function
title: postfix_style_mail_log_path
resource: LPE-CT/src/smtp/audit.rs#L111-L137
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log
---

# Signature

`fn postfix_style_mail_log_path() -> Option<PathBuf>`

# Called by

- [append_postfix_style_mail_log](../../../../../functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log.md)