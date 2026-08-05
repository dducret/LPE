---
type: Rust Method
title: handle_contact_query
resource: crates/lpe-jmap/src/contacts.rs#L218-L269
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/validation/validate_entity_sort
  - functions/crates/lpe-jmap/src/validation/validate_contact_filter
  - functions/crates/lpe-jmap/src/contacts/contact_matches_filter
  - functions/crates/lpe-jmap/src/state/query_position
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_contact_query( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [validate_entity_sort](../../../../../../functions/crates/lpe-jmap/src/validation/validate_entity_sort.md)
- [validate_contact_filter](../../../../../../functions/crates/lpe-jmap/src/validation/validate_contact_filter.md)
- [contact_matches_filter](../../../../../../functions/crates/lpe-jmap/src/contacts/contact_matches_filter.md)
- [query_position](../../../../../../functions/crates/lpe-jmap/src/state/query_position.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)