---
type: Rust Method
title: tracked_mail_processing_message_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1101-L1108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id
---

# Signature

`pub(crate) fn tracked_mail_processing_message_for_id( &self, message_id: u64, ) -> Option<&MapiMessage>`

# Calls

- [tracked_mail_processing_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/tracked_mail_processing_messages.md)

# Called by

- [search_folder_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folder_message_for_id.md)