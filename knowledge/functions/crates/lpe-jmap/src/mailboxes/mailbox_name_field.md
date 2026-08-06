---
type: Rust Function
title: mailbox_name_field
resource: crates/lpe-jmap/src/mailboxes.rs#L790-L797
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names
---

# Signature

`fn mailbox_name_field<'a>(value: &'a Value, object_error: &str) -> Result<&'a str>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [validate_mailbox_set_names](../../../../../functions/crates/lpe-jmap/src/mailboxes/validate_mailbox_set_names.md)