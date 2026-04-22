use lpe_magika::{
    write_validation_record, Detector, ExpectedKind, IngressContext, PolicyDecision,
    ValidationRequest, Validator,
};
use std::env;
use std::path::{Path, PathBuf};

pub(crate) fn pst_import_dir() -> PathBuf {
    env::var("LPE_PST_IMPORT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/lib/lpe/imports"))
}

pub(crate) fn pst_upload_max_bytes() -> usize {
    env::var("LPE_PST_UPLOAD_MAX_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(20 * 1024 * 1024 * 1024)
}

pub(crate) fn validate_uploaded_pst_file(
    path: &Path,
    file_name: &str,
    declared_mime: Option<&str>,
) -> anyhow::Result<()> {
    validate_uploaded_pst_file_with_validator(
        &Validator::from_env(),
        path,
        file_name,
        declared_mime,
    )
}

pub(crate) fn validate_uploaded_pst_file_with_validator<D: Detector>(
    validator: &Validator<D>,
    path: &Path,
    file_name: &str,
    declared_mime: Option<&str>,
) -> anyhow::Result<()> {
    let request = ValidationRequest {
        ingress_context: IngressContext::PstUpload,
        declared_mime: declared_mime.map(ToString::to_string),
        filename: Some(file_name.to_string()),
        expected_kind: ExpectedKind::Pst,
    };
    let outcome = validator.validate_path(request.clone(), path)?;
    if outcome.policy_decision != PolicyDecision::Accept {
        let _ = std::fs::remove_file(path);
        return Err(anyhow::anyhow!(
            "PST upload blocked by Magika validation: {}",
            outcome.reason
        ));
    }

    write_validation_record(path, &request, &outcome, std::fs::metadata(path)?.len())?;
    Ok(())
}

pub(crate) fn sanitize_upload_filename(file_name: &str) -> String {
    let basename = Path::new(file_name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("mailbox.pst");
    let sanitized: String = basename
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "mailbox.pst".to_string()
    } else {
        sanitized
    }
}
