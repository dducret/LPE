---
type: Rust Function
title: normalize_login_name
resource: crates/lpe-domain/src/normalization.rs#L71-L77
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase
---

# Signature

`pub fn normalize_login_name(username: &str, hinted_user: Option<&str>) -> String`

# Calls

- [normalize_trimmed_lowercase](../../../../../functions/crates/lpe-domain/src/normalization/normalize_trimmed_lowercase.md)