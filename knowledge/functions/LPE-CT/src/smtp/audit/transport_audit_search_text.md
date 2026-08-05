---
type: Rust Function
title: transport_audit_search_text
resource: LPE-CT/src/smtp/audit.rs#L278-L308
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event
---

# Signature

`fn transport_audit_search_text(event: &TransportAuditEvent) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [persist_transport_audit_db_event](../../../../../functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event.md)