---
type: Rust Function
title: insert_ews_im_member_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1493-L1622
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn insert_ews_im_member_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, principal: &AccountPrincipal, group_id: Uuid, member: &EwsImMemberInput, ) -> Result<sqlx::postgres::PgRow>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)