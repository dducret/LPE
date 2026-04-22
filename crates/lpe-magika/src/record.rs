use anyhow::{Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::types::{PersistedValidationRecord, ValidationOutcome, ValidationRequest};

pub fn write_validation_record(
    path: &Path,
    request: &ValidationRequest,
    outcome: &ValidationOutcome,
    file_size: u64,
) -> Result<PathBuf> {
    let record = PersistedValidationRecord {
        version: 1,
        created_at: unix_timestamp(),
        ingress_context: request.ingress_context,
        file_size,
        policy_decision: outcome.policy_decision,
        expected_kind: request.expected_kind,
        outcome: outcome.clone(),
    };
    let sidecar = validation_sidecar_path(path);
    fs::write(&sidecar, serde_json::to_vec_pretty(&record)?)
        .with_context(|| format!("write validation sidecar {}", sidecar.display()))?;
    Ok(sidecar)
}

pub fn read_validation_record(path: &Path) -> Result<PersistedValidationRecord> {
    let sidecar = validation_sidecar_path(path);
    let bytes = fs::read(&sidecar)
        .with_context(|| format!("read validation sidecar {}", sidecar.display()))?;
    serde_json::from_slice(&bytes).context("parse validation sidecar JSON")
}

pub fn validation_sidecar_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".magika.json");
    PathBuf::from(value)
}

fn unix_timestamp() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => format!("unix:{}", duration.as_secs()),
        Err(_) => "unix:0".to_string(),
    }
}
