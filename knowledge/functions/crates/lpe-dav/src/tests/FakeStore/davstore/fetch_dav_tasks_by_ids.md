---
type: Rust Method
title: fetch_dav_tasks_by_ids
resource: crates/lpe-dav/src/tests.rs#L406-L420
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_dav_tasks_by_ids<'a>( &'a self, _principal_account_id: Uuid, ids: &'a [Uuid], ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>>`