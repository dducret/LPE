---
type: Rust Function
title: parse_detection_json
resource: crates/lpe-magika/src/detection.rs#L6-L78
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-magika/src/system/SystemDetector/detector/detect
---

# Signature

`pub(crate) fn parse_detection_json(raw: Value) -> Result<MagikaDetection>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [detect](../../../../../functions/crates/lpe-magika/src/system/SystemDetector/detector/detect.md)