use axum::http::StatusCode;
use serde::Serialize;
use std::{
    env, fs, io,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

const PREVIEW_LIMIT_BYTES: usize = 1024 * 1024;

#[derive(Debug, Serialize)]
pub(crate) struct HostLogList {
    pub(crate) category: String,
    pub(crate) items: Vec<HostLogItem>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HostLogItem {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) modified_at_unix_seconds: Option<u64>,
    pub(crate) size_bytes: u64,
    pub(crate) exists: bool,
    pub(crate) previewable: bool,
    pub(crate) downloadable: bool,
    pub(crate) deletable: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct HostLogContent {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) content: String,
    pub(crate) truncated: bool,
    pub(crate) size_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct HostLogDownload {
    pub(crate) name: String,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct HostLogError {
    status: StatusCode,
    message: String,
}

impl HostLogError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

pub(crate) fn list(category: &str) -> Result<HostLogList, HostLogError> {
    let definition = category_definition(category)?;
    let log_dir = host_log_dir();
    let mut items = Vec::new();

    for name in discover_log_names(&log_dir, definition.current_name)? {
        let path = log_dir.join(&name);
        items.push(item_for_name(&name, &path));
    }

    if items.is_empty() {
        items.push(virtual_item(definition.current_name));
    }

    items.sort_by(|left, right| match (left.exists, right.exists) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => right
            .modified_at_unix_seconds
            .cmp(&left.modified_at_unix_seconds)
            .then_with(|| left.name.cmp(&right.name)),
    });

    Ok(HostLogList {
        category: definition.id.to_string(),
        items,
    })
}

pub(crate) fn read_content(category: &str, id: &str) -> Result<HostLogContent, HostLogError> {
    let target = resolve_log(category, id)?;
    if !target.exists {
        return Ok(HostLogContent {
            id: target.name.clone(),
            name: target.name,
            content: String::new(),
            truncated: false,
            size_bytes: 0,
        });
    }
    if !is_previewable(&target.name) {
        return Err(HostLogError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "compressed log previews are not supported; download the file instead",
        ));
    }

    let bytes = fs::read(&target.path)
        .map_err(|error| io_error(StatusCode::INTERNAL_SERVER_ERROR, "read log file", error))?;
    let truncated = bytes.len() > PREVIEW_LIMIT_BYTES;
    let preview_bytes = if truncated {
        &bytes[..PREVIEW_LIMIT_BYTES]
    } else {
        &bytes
    };
    Ok(HostLogContent {
        id: target.name.clone(),
        name: target.name,
        content: String::from_utf8_lossy(preview_bytes).to_string(),
        truncated,
        size_bytes: bytes.len() as u64,
    })
}

pub(crate) fn download(category: &str, id: &str) -> Result<HostLogDownload, HostLogError> {
    let target = resolve_log(category, id)?;
    if !target.exists {
        return Ok(HostLogDownload {
            name: target.name,
            bytes: Vec::new(),
        });
    }
    let bytes = fs::read(&target.path)
        .map_err(|error| io_error(StatusCode::INTERNAL_SERVER_ERROR, "read log file", error))?;
    Ok(HostLogDownload {
        name: target.name,
        bytes,
    })
}

pub(crate) fn delete(category: &str, id: &str) -> Result<String, HostLogError> {
    let target = resolve_log(category, id)?;
    if !target.exists {
        return Err(HostLogError::new(
            StatusCode::NOT_FOUND,
            "log file not found",
        ));
    }
    fs::remove_file(&target.path)
        .map_err(|error| io_error(StatusCode::INTERNAL_SERVER_ERROR, "delete log file", error))?;
    Ok(target.name)
}

fn host_log_dir() -> PathBuf {
    env::var("LPE_CT_HOST_LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/var/log"))
}

fn category_definition(category: &str) -> Result<LogCategory, HostLogError> {
    match category {
        "mail" => Ok(LogCategory {
            id: "mail",
            current_name: "mail.log",
        }),
        "interface" => Ok(LogCategory {
            id: "interface",
            current_name: "CFMA.log",
        }),
        "messages" => Ok(LogCategory {
            id: "messages",
            current_name: "dmesg",
        }),
        _ => Err(HostLogError::new(
            StatusCode::BAD_REQUEST,
            "unknown host log category",
        )),
    }
}

fn discover_log_names(log_dir: &Path, current_name: &str) -> Result<Vec<String>, HostLogError> {
    let mut names = Vec::new();
    match fs::read_dir(log_dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry.map_err(|error| {
                    io_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "read log directory",
                        error,
                    )
                })?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name == current_name || name.starts_with(&format!("{current_name}.")) {
                    names.push(name);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(io_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "read log directory",
                error,
            ))
        }
    }

    if names.is_empty() && log_dir.join(current_name).exists() {
        names.push(current_name.to_string());
    }
    Ok(names)
}

fn item_for_name(name: &str, path: &Path) -> HostLogItem {
    let metadata = fs::metadata(path).ok();
    let exists = metadata.as_ref().is_some_and(|meta| meta.is_file());
    HostLogItem {
        id: name.to_string(),
        name: name.to_string(),
        modified_at_unix_seconds: metadata
            .as_ref()
            .and_then(|meta| meta.modified().ok())
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs()),
        size_bytes: metadata.as_ref().map_or(0, |meta| meta.len()),
        exists,
        previewable: exists && is_previewable(name),
        downloadable: true,
        deletable: exists,
    }
}

fn virtual_item(name: &str) -> HostLogItem {
    HostLogItem {
        id: name.to_string(),
        name: name.to_string(),
        modified_at_unix_seconds: None,
        size_bytes: 0,
        exists: false,
        previewable: true,
        downloadable: true,
        deletable: false,
    }
}

fn resolve_log(category: &str, id: &str) -> Result<ResolvedLog, HostLogError> {
    let definition = category_definition(category)?;
    if !is_allowed_log_name(definition.current_name, id) {
        return Err(HostLogError::new(
            StatusCode::BAD_REQUEST,
            "log file is outside the allowed host log set",
        ));
    }
    let log_dir = host_log_dir();
    let path = log_dir.join(id);
    let metadata = fs::metadata(&path).ok();
    let exists = metadata.as_ref().is_some_and(|meta| meta.is_file());
    Ok(ResolvedLog {
        name: id.to_string(),
        path,
        exists,
    })
}

fn is_allowed_log_name(current_name: &str, name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && (name == current_name || name.starts_with(&format!("{current_name}.")))
}

fn is_previewable(name: &str) -> bool {
    !name.ends_with(".gz") && !name.ends_with(".xz") && !name.ends_with(".zip")
}

fn io_error(status: StatusCode, action: &str, error: io::Error) -> HostLogError {
    HostLogError::new(status, format!("unable to {action}: {error}"))
}

struct LogCategory {
    id: &'static str,
    current_name: &'static str,
}

struct ResolvedLog {
    name: String,
    path: PathBuf,
    exists: bool,
}
