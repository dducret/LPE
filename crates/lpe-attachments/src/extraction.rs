use anyhow::{anyhow, bail, Context, Result};
use lpe_magika::{ExpectedKind, IngressContext, PolicyDecision, ValidationRequest, Validator};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::fs::{self};
use std::io::Read;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use zip::ZipArchive;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentFormat {
    Pdf,
    Docx,
    Odt,
}

impl AttachmentFormat {
    fn from_detected_mime(mime_type: &str) -> Result<Self> {
        match mime_type {
            "application/pdf" => Ok(Self::Pdf),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
                Ok(Self::Docx)
            }
            "application/vnd.oasis.opendocument.text" => Ok(Self::Odt),
            other => bail!("unsupported validated attachment format: {other}"),
        }
    }
}

pub fn extract_text_from_path(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    let outcome = Validator::from_env().validate_path(
        ValidationRequest {
            ingress_context: IngressContext::AttachmentParsing,
            declared_mime: None,
            filename: path
                .file_name()
                .and_then(|value| value.to_str())
                .map(ToString::to_string),
            expected_kind: ExpectedKind::SupportedAttachmentText,
        },
        path,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "attachment validation blocked extraction: {}",
            outcome.reason
        );
    }

    match AttachmentFormat::from_detected_mime(&outcome.detected_mime)? {
        AttachmentFormat::Pdf => extract_pdf_text(path),
        AttachmentFormat::Docx => extract_docx_text(path),
        AttachmentFormat::Odt => extract_odt_text(path),
    }
}

pub fn extract_text_from_bytes(
    bytes: &[u8],
    declared_mime: Option<&str>,
    filename: Option<&str>,
) -> Result<String> {
    let outcome = Validator::from_env().validate_bytes(
        ValidationRequest {
            ingress_context: IngressContext::AttachmentParsing,
            declared_mime: declared_mime.map(ToString::to_string),
            filename: filename.map(ToString::to_string),
            expected_kind: ExpectedKind::SupportedAttachmentText,
        },
        bytes,
    )?;
    if outcome.policy_decision != PolicyDecision::Accept {
        bail!(
            "attachment validation blocked extraction: {}",
            outcome.reason
        );
    }

    match AttachmentFormat::from_detected_mime(&outcome.detected_mime)? {
        AttachmentFormat::Pdf => extract_pdf_text_from_bytes(bytes),
        AttachmentFormat::Docx => extract_docx_text_from_bytes(bytes),
        AttachmentFormat::Odt => extract_odt_text_from_bytes(bytes),
    }
}

fn extract_pdf_text(path: &Path) -> Result<String> {
    let mut document = pdf_oxide::PdfDocument::open(path)
        .with_context(|| format!("open PDF at {}", path.display()))?;
    let mut pages = Vec::new();

    for page_index in 0..document.page_count()? {
        let text = document
            .extract_text(page_index)
            .with_context(|| format!("extract PDF page {page_index}"))?;
        let text = normalize_whitespace(&text);
        if !text.is_empty() {
            pages.push(text);
        }
    }

    Ok(pages.join("\n\n"))
}

fn extract_pdf_text_from_bytes(bytes: &[u8]) -> Result<String> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp_path = std::env::temp_dir().join(format!("lpe-attachment-{suffix}.pdf"));
    fs::write(&temp_path, bytes)
        .with_context(|| format!("write temporary PDF at {}", temp_path.display()))?;
    let result = extract_pdf_text(&temp_path);
    let _ = fs::remove_file(&temp_path);
    result
}

fn extract_docx_text(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("read DOCX at {}", path.display()))?;
    extract_docx_text_from_bytes(&bytes)
        .with_context(|| format!("extract DOCX text from {}", path.display()))
}

fn extract_docx_text_from_bytes(bytes: &[u8]) -> Result<String> {
    let text = docx_lite::extract_text_from_bytes(bytes).context("extract DOCX text from bytes")?;
    Ok(normalize_whitespace(&text))
}

fn extract_odt_text(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("read ODT at {}", path.display()))?;
    extract_odt_text_from_bytes(&bytes)
        .with_context(|| format!("extract ODT text from {}", path.display()))
}

fn extract_odt_text_from_bytes(bytes: &[u8]) -> Result<String> {
    let cursor = std::io::Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor).context("read ODT ZIP container from bytes")?;
    let mut content_xml = String::new();

    archive
        .by_name("content.xml")
        .context("read content.xml from ODT bytes")?
        .read_to_string(&mut content_xml)
        .context("decode ODT content.xml as UTF-8")?;

    extract_odt_content_xml(&content_xml)
}

fn extract_odt_content_xml(xml: &str) -> Result<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut output = String::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Text(text)) => {
                let decoded = text
                    .xml_content()
                    .map_err(|error| anyhow!("decode ODT text node: {error}"))?;
                append_text(&mut output, &decoded);
            }
            Ok(Event::End(end)) => {
                if matches!(
                    local_name(end.name().as_ref()),
                    b"p" | b"h" | b"list-item" | b"table-row"
                ) {
                    ensure_paragraph_break(&mut output);
                }
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(error) => return Err(anyhow!("parse ODT XML: {error}")),
        }

        buf.clear();
    }

    Ok(normalize_whitespace(&output))
}

fn local_name(name: &[u8]) -> &[u8] {
    name.rsplit(|byte| *byte == b':').next().unwrap_or(name)
}

fn append_text(output: &mut String, text: &str) {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return;
    }

    if !output.is_empty() && !output.ends_with([' ', '\n']) {
        output.push(' ');
    }

    output.push_str(trimmed);
}

fn ensure_paragraph_break(output: &mut String) {
    if output.is_empty() {
        return;
    }

    if !output.ends_with("\n\n") {
        if output.ends_with('\n') {
            output.push('\n');
        } else {
            output.push_str("\n\n");
        }
    }
}

fn normalize_whitespace(input: &str) -> String {
    input
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::AttachmentFormat;

    #[test]
    fn validated_attachment_format_matches_supported_v1_scope() {
        assert_eq!(
            AttachmentFormat::from_detected_mime("application/pdf").unwrap(),
            AttachmentFormat::Pdf
        );
        assert_eq!(
            AttachmentFormat::from_detected_mime(
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
            .unwrap(),
            AttachmentFormat::Docx
        );
        assert_eq!(
            AttachmentFormat::from_detected_mime("application/vnd.oasis.opendocument.text")
                .unwrap(),
            AttachmentFormat::Odt
        );
        assert!(AttachmentFormat::from_detected_mime("image/png").is_err());
    }
}
