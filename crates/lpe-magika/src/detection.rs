use anyhow::{anyhow, bail, Result};
use serde_json::Value;

use crate::types::MagikaDetection;

pub(crate) fn parse_detection_json(raw: Value) -> Result<MagikaDetection> {
    let entry = raw
        .as_array()
        .and_then(|entries| entries.first())
        .ok_or_else(|| anyhow!("Magika JSON output is not a non-empty array"))?;
    let result = entry
        .get("result")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Magika JSON output is missing result"))?;
    let status = result
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if status != "ok" {
        bail!("Magika returned non-ok status: {status}");
    }
    let value = result
        .get("value")
        .ok_or_else(|| anyhow!("Magika JSON output is missing result value"))?;
    let output = value
        .get("output")
        .and_then(Value::as_object)
        .or_else(|| value.as_object())
        .ok_or_else(|| anyhow!("Magika JSON output is missing output"))?;
    let label = output
        .get("label")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mime_type = output
        .get("mime_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let description = output
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let group = output
        .get("group")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let extensions = output
        .get("extensions")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let score = value
        .get("score")
        .and_then(Value::as_f64)
        .map(|value| value as f32);

    if label.trim().is_empty() || mime_type.trim().is_empty() {
        bail!("Magika returned an incomplete detection result");
    }

    Ok(MagikaDetection {
        label,
        mime_type,
        description,
        group,
        extensions,
        score,
    })
}
