---
type: Rust Module
title: system
resource: crates/lpe-magika/src/system.rs#L1-L90
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-context-result
  - external/serde-json-value
  - external/std-env-path-pathbuf-process-command-stdio
  - external/crate-constants-default-magika-min-score-detection-parse-detection-json-types-detectionsource-detector-magikadetection
  - external/std-io-write
  member_of:
  - packages/crates/lpe-magika
---

# Contains

- [SystemDetector](../../../../classes/crates/lpe-magika/src/system/SystemDetector.md)
- [from_env](../../../../functions/crates/lpe-magika/src/system/SystemDetector/from_env.md)
- [min_score](../../../../functions/crates/lpe-magika/src/system/SystemDetector/min_score.md)
- [run_magika](../../../../functions/crates/lpe-magika/src/system/SystemDetector/run_magika.md)
- [detect](../../../../functions/crates/lpe-magika/src/system/SystemDetector/detector/detect.md)

# Imports

- `anyhow::{anyhow, bail, Context, Result}`
- `serde_json::Value`
- `std::{
    env,
    path::PathBuf,
    process::{Command, Stdio},
}`
- `crate::{
    constants::DEFAULT_MAGIKA_MIN_SCORE,
    detection::parse_detection_json,
    types::{DetectionSource, Detector, MagikaDetection},
}`
- `std::io::Write`

# Member of

- [lpe-magika](../../../../packages/crates/lpe-magika.md)