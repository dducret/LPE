---
type: Rust Method
title: put_sieve_script
resource: crates/lpe-managesieve/src/store.rs#L64-L76
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn put_sieve_script<'a>( &'a self, account_id: Uuid, name: &'a str, content: &'a str, activate: bool, audit: AuditEntryInput, ) -> StoreFuture<'a, SieveScriptDocument>`