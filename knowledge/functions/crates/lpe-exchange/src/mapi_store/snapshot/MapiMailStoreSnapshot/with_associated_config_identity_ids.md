---
type: Rust Method
title: with_associated_config_identity_ids
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L362-L369
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_identity_only_placeholder_does_not_open_without_backing_message
  - functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection
  - functions/crates/lpe-exchange/src/mapi_store/tests/modeled_virtual_associated_config_identity_opens_via_dynamic_id
---

# Signature

`pub(crate) fn with_associated_config_identity_ids( mut self, ids: Vec<MapiAssociatedConfigIdentity>, ) -> Self`

# Calls

- [apply_associated_config_identities](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities.md)

# Called by

- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [contacts_project_exactly_the_persisted_contact_link_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai.md)
- [associated_config_identity_only_placeholder_does_not_open_without_backing_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_identity_only_placeholder_does_not_open_without_backing_message.md)
- [distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection.md)
- [modeled_virtual_associated_config_identity_opens_via_dynamic_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/modeled_virtual_associated_config_identity_opens_via_dynamic_id.md)