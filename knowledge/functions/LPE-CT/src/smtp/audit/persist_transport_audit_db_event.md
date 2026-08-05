---
type: Rust Function
title: persist_transport_audit_db_event
resource: LPE-CT/src/smtp/audit.rs#L203-L261
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/smtp/audit/transport_audit_event_key
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/LPE-CT/src/smtp/audit/transport_audit_search_text
  called_by:
  - functions/LPE-CT/src/smtp/audit/append_transport_audit
---

# Signature

`async fn persist_transport_audit_db_event( pool: &PgPool, event: &TransportAuditEvent, ) -> Result<()>`

# Calls

- [transport_audit_event_key](../../../../../functions/LPE-CT/src/smtp/audit/transport_audit_event_key.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [transport_audit_search_text](../../../../../functions/LPE-CT/src/smtp/audit/transport_audit_search_text.md)

# Called by

- [append_transport_audit](../../../../../functions/LPE-CT/src/smtp/audit/append_transport_audit.md)