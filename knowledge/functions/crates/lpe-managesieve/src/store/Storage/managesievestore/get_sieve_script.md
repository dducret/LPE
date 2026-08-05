---
type: Rust Method
title: get_sieve_script
resource: crates/lpe-managesieve/src/store.rs#L56-L62
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn get_sieve_script<'a>( &'a self, account_id: Uuid, name: &'a str, ) -> StoreFuture<'a, Option<SieveScriptDocument>>`