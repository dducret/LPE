---
type: Rust Module
title: audit
resource: LPE-CT/src/smtp/audit.rs#L1-L351
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/std-io-write
  member_of:
  - packages/LPE-CT
---

# Contains

- [TransportAuditEvent](../../../../classes/LPE-CT/src/smtp/audit/TransportAuditEvent.md)
- [append_transport_audit](../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)
- [append_postfix_style_mail_log](../../../../functions/LPE-CT/src/smtp/audit/append_postfix_style_mail_log.md)
- [postfix_style_mail_log_path](../../../../functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_path.md)
- [postfix_style_mail_log_line](../../../../functions/LPE-CT/src/smtp/audit/postfix_style_mail_log_line.md)
- [sanitize_mail_log_value](../../../../functions/LPE-CT/src/smtp/audit/sanitize_mail_log_value.md)
- [persist_transport_audit_db_event](../../../../functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event.md)
- [transport_audit_event_key](../../../../functions/LPE-CT/src/smtp/audit/transport_audit_event_key.md)
- [transport_audit_search_text](../../../../functions/LPE-CT/src/smtp/audit/transport_audit_search_text.md)
- [quarantine_search_text](../../../../functions/LPE-CT/src/smtp/audit/quarantine_search_text.md)

# Imports

- `super::*`
- `std::io::Write`

# Member of

- [lpe-ct](../../../../packages/LPE-CT.md)