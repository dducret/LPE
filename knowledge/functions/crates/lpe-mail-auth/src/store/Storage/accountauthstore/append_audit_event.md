---
type: Rust Method
title: append_audit_event
resource: crates/lpe-mail-auth/src/store.rs#L62-L68
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn append_audit_event<'a>( &'a self, tenant_id: &'a Uuid, entry: AuditEntryInput, ) -> StoreFuture<'a, ()>`