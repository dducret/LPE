---
type: Rust Function
title: parse_email_copy
resource: crates/lpe-jmap/src/drafts.rs#L72-L95
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  - functions/crates/lpe-jmap/src/parse/parse_uuid
  called_by:
  - functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy
---

# Signature

`pub(crate) fn parse_email_copy( value: Value, created_ids: &HashMap<String, String>, ) -> Result<(Uuid, Uuid)>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [resolve_creation_reference](../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [as_bool](../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)
- [parse_uuid](../../../../../functions/crates/lpe-jmap/src/parse/parse_uuid.md)

# Called by

- [handle_email_copy](../../../../../functions/crates/lpe-jmap/src/mail/JmapService/handle_email_copy.md)