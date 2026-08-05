---
type: Rust Method
title: run_magika
resource: crates/lpe-magika/src/system.rs#L38-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-magika/src/system/SystemDetector/detector/detect
---

# Signature

`fn run_magika(&self, source: DetectionSource<'_>) -> Result<Value>`

# Calls

- [context](../../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [detect](../../../../../../functions/crates/lpe-magika/src/system/SystemDetector/detector/detect.md)