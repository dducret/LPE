---
type: Rust Method
title: detect
resource: crates/lpe-magika/src/system.rs#L86-L89
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-magika/src/system/SystemDetector/run_magika
  - functions/crates/lpe-magika/src/detection/parse_detection_json
---

# Signature

`fn detect(&self, source: DetectionSource<'_>) -> Result<MagikaDetection>`

# Calls

- [run_magika](../../../../../../../functions/crates/lpe-magika/src/system/SystemDetector/run_magika.md)
- [parse_detection_json](../../../../../../../functions/crates/lpe-magika/src/detection/parse_detection_json.md)