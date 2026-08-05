---
type: Rust Method
title: set_active_sieve_script
resource: crates/lpe-managesieve/src/store.rs#L100-L107
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn set_active_sieve_script<'a>( &'a self, account_id: Uuid, name: Option<&'a str>, audit: AuditEntryInput, ) -> StoreFuture<'a, Option<String>>`