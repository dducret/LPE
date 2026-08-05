---
type: Rust Method
title: fetch_activesync_email_states
resource: crates/lpe-activesync/src/tests.rs#L831-L889
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn fetch_activesync_email_states<'a>( &'a self, _account_id: Uuid, mailbox_id: Uuid, position: u64, limit: u64, ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>`