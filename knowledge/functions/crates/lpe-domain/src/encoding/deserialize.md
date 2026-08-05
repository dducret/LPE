---
type: Rust Function
title: deserialize
resource: crates/lpe-domain/src/encoding.rs#L15-L21
generated:
  by: okf-rs/0.3.0
---

# Signature

`pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error> where D: Deserializer<'de>,`