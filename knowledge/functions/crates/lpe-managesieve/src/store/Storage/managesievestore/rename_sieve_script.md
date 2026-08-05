---
type: Rust Method
title: rename_sieve_script
resource: crates/lpe-managesieve/src/store.rs#L87-L98
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn rename_sieve_script<'a>( &'a self, account_id: Uuid, old_name: &'a str, new_name: &'a str, audit: AuditEntryInput, ) -> StoreFuture<'a, SieveScriptSummary>`