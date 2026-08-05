---
type: Rust Function
title: mapi_folder_identity_requests
resource: crates/lpe-exchange/src/mapi_store/folder_versions.rs#L54-L91
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox
  - functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_identity_requests_for_mailboxes
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/mapi_identity_requests
  - functions/crates/lpe-exchange/src/mapi_store/folder_versions/canonical_mailbox_owns_reserved_fid_and_virtual_folders_fill_the_gaps
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
---

# Signature

`pub(crate) fn mapi_folder_identity_requests(mailboxes: &[JmapMailbox]) -> Vec<MapiIdentityRequest>`

# Calls

- [is_virtual_special_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_store/is_virtual_special_mailbox.md)
- [reserved_folder_counter_for_role](../../../../../../functions/crates/lpe-exchange/src/mapi_store/reserved_folder_counter_for_role.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [virtual_special_mailbox_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/virtual_special_mailbox_id.md)

# Called by

- [mapi_identity_requests_for_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/mapi_identity_requests_for_mailboxes.md)
- [load_mapi_mail_store](../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [mapi_identity_requests](../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_identity_requests.md)
- [canonical_mailbox_owns_reserved_fid_and_virtual_folders_fill_the_gaps](../../../../../../functions/crates/lpe-exchange/src/mapi_store/folder_versions/canonical_mailbox_owns_reserved_fid_and_virtual_folders_fill_the_gaps.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)