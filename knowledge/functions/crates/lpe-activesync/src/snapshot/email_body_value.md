---
type: Rust Function
title: email_body_value
resource: crates/lpe-activesync/src/snapshot.rs#L134-L176
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/truncate_body_bytes
---

# Signature

`fn email_body_value( email: &JmapEmail, body_preference: &BodyPreference, mime_blob: Option<&JmapUploadBlob>, ) -> Value`

# Calls

- [truncate_body_bytes](../../../../../functions/crates/lpe-activesync/src/snapshot/truncate_body_bytes.md)