use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use std::{
    env,
    path::PathBuf,
    process::{Command, Stdio},
};

use crate::{
    constants::DEFAULT_MAGIKA_MIN_SCORE,
    detection::parse_detection_json,
    types::{DetectionSource, Detector, MagikaDetection},
};

#[derive(Debug, Clone)]
pub struct SystemDetector {
    command: PathBuf,
    min_score: f32,
}

impl SystemDetector {
    pub fn from_env() -> Self {
        let command = env::var("LPE_MAGIKA_BIN").unwrap_or_else(|_| "magika".to_string());
        let min_score = env::var("LPE_MAGIKA_MIN_SCORE")
            .ok()
            .and_then(|value| value.parse::<f32>().ok())
            .unwrap_or(DEFAULT_MAGIKA_MIN_SCORE);
        Self {
            command: PathBuf::from(command),
            min_score,
        }
    }

    pub fn min_score(&self) -> f32 {
        self.min_score
    }

    fn run_magika(&self, source: DetectionSource<'_>) -> Result<Value> {
        let mut command = Command::new(&self.command);
        command.arg("--json");
        match source {
            DetectionSource::Bytes(bytes) => {
                command.arg("-");
                command.stdin(Stdio::piped());
                command.stdout(Stdio::piped());
                let mut child = command
                    .spawn()
                    .with_context(|| format!("spawn Magika command {}", self.command.display()))?;
                {
                    let stdin = child
                        .stdin
                        .as_mut()
                        .ok_or_else(|| anyhow!("Magika stdin is unavailable"))?;
                    use std::io::Write;
                    stdin.write_all(bytes)?;
                }
                let output = child.wait_with_output()?;
                if !output.status.success() {
                    bail!(
                        "Magika command failed with status {}: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                serde_json::from_slice(&output.stdout).context("parse Magika JSON output")
            }
            DetectionSource::Path(path) => {
                command.arg(path);
                let output = command
                    .output()
                    .with_context(|| format!("run Magika on {}", path.display()))?;
                if !output.status.success() {
                    bail!(
                        "Magika command failed with status {}: {}",
                        output.status,
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
                serde_json::from_slice(&output.stdout).context("parse Magika JSON output")
            }
        }
    }
}

impl Detector for SystemDetector {
    fn detect(&self, source: DetectionSource<'_>) -> Result<MagikaDetection> {
        let raw = self.run_magika(source)?;
        parse_detection_json(raw)
    }
}
