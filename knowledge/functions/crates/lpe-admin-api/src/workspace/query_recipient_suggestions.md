---
type: Rust Function
title: query_recipient_suggestions
resource: crates/lpe-admin-api/src/workspace.rs#L756-L768
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-admin-api/src/access/require_account
---

# Signature

`pub(crate) async fn query_recipient_suggestions( State(storage): State<Storage>, headers: HeaderMap, Query(request): Query<RecipientSuggestionQuery>, ) -> ApiResult<Vec<RecipientSuggestion>>`

# Calls

- [require_account](../../../../../functions/crates/lpe-admin-api/src/access/require_account.md)