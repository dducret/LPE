---
type: Rust Method
title: dismiss_reminder_occurrence
resource: crates/lpe-exchange/src/tests/mod.rs#L10652-L10673
visibility: private
generated:
  by: okf-rs/0.3.0
---

# Signature

`fn dismiss_reminder_occurrence<'a>( &'a self, _account_id: Uuid, source_type: &'a str, source_id: Uuid, occurrence_start_at: Option<&'a str>, dismissed_at: &'a str, ) -> StoreFuture<'a, ()>`