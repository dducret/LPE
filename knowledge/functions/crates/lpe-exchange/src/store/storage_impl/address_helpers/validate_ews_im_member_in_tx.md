---
type: Rust Function
title: validate_ews_im_member_in_tx
resource: crates/lpe-exchange/src/store/storage_impl/address_helpers.rs#L1364-L1440
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`async fn validate_ews_im_member_in_tx( tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, principal: &AccountPrincipal, member: &EwsImMemberInput, ) -> Result<()>`