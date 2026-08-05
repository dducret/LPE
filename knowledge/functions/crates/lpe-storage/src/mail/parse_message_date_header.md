---
type: Rust Function
title: parse_message_date_header
resource: crates/lpe-storage/src/mail.rs#L169-L174
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mail/parse_header_records
  - functions/crates/lpe-storage/src/mail/parse_mail_datetime
  called_by:
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
---

# Signature

`pub fn parse_message_date_header(raw_message: &[u8]) -> Option<String>`

# Calls

- [parse_header_records](../../../../../functions/crates/lpe-storage/src/mail/parse_header_records.md)
- [parse_mail_datetime](../../../../../functions/crates/lpe-storage/src/mail/parse_mail_datetime.md)

# Called by

- [store_inbound_message_in_tx](../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)