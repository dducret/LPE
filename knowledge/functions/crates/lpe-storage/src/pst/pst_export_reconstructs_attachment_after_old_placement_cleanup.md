---
type: Rust Function
title: pst_export_reconstructs_attachment_after_old_placement_cleanup
resource: crates/lpe-storage/src/pst.rs#L868-L911
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source
  - functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn pst_export_reconstructs_attachment_after_old_placement_cleanup()`

# Calls

- [insert_message_with_attachment](../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [migrate_attachment_and_cleanup_source](../../../../../functions/crates/lpe-storage/src/pst/migrate_attachment_and_cleanup_source.md)
- [export_mailbox_to_pst](../../../../../functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)