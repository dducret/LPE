---
type: Rust Function
title: ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L3646-L3975
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows
  - functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid
---

# Signature

`fn ipm_subtree_hierarchy_suppresses_mail_folders_shadowing_outlook_special_folders()`

# Calls

- [remember_mapi_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity.md)
- [hierarchy_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_rows.md)
- [sync_mailboxes_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/sync_mailboxes_for.md)
- [hierarchy_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_id.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [serialize_hierarchy_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/serialize_hierarchy_row.md)
- [serialize_advertised_special_folder_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_mailbox_guid.md)