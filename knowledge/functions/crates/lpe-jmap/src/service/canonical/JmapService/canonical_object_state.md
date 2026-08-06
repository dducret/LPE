---
type: Rust Method
title: canonical_object_state
resource: crates/lpe-jmap/src/service/canonical.rs#L459-L519
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint
  - functions/crates/lpe-jmap/src/state/encode_state
  - functions/crates/lpe-jmap/src/state/encode_state_with_cursor
  - functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
  - functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write
  - functions/crates/lpe-jmap/src/tests/canonical_jmap_object_families_expose_full_method_matrix_without_mapi_session_objects
---

# Signature

`pub(crate) async fn canonical_object_state( &self, account: &AuthenticatedAccount, account_id: Uuid, data_type: &str, ) -> Result<String>`

# Calls

- [identity_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/identity_object_state.md)
- [email_submission_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/email_submission_object_state.md)
- [requested_account_access](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [mailbox_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mailbox_object_state.md)
- [mail_object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/mail_object_state.md)
- [canonical_objects](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [opaque_state_fingerprint](../../../../../../../functions/crates/lpe-jmap/src/service/helpers/opaque_state_fingerprint.md)
- [encode_state](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state.md)
- [encode_state_with_cursor](../../../../../../../functions/crates/lpe-jmap/src/state/encode_state_with_cursor.md)
- [object_state](../../../../../../../functions/crates/lpe-jmap/src/service/object_state/JmapService/object_state.md)

# Called by

- [handle_reminder_set](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_set.md)
- [handle_share_set](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)
- [handle_search_folder_set](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_search_folder_set.md)
- [handle_canonical_unsupported_write](../../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/handle_canonical_unsupported_write.md)
- [canonical_jmap_object_families_expose_full_method_matrix_without_mapi_session_objects](../../../../../../../functions/crates/lpe-jmap/src/tests/canonical_jmap_object_families_expose_full_method_matrix_without_mapi_session_objects.md)