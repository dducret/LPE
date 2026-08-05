---
type: Rust Function
title: transport_audit_event_key
resource: LPE-CT/src/smtp/audit.rs#L263-L276
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event
---

# Signature

`fn transport_audit_event_key(event: &TransportAuditEvent) -> String`

# Called by

- [persist_transport_audit_db_event](../../../../../functions/LPE-CT/src/smtp/audit/persist_transport_audit_db_event.md)