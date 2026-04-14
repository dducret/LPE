use anyhow::{anyhow, bail, Context, Result};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use zip::ZipArchive;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentFormat {
    Pdf,
    Docx,
    Odt,
}

impl AttachmentFormat {
    pub fn from_path(path: &Path) -> Result<Self> {
        match path
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| value.to_ascii_lowercase())
            .as_deref()
        {
            Some("pdf") => Ok(Self::Pdf),
            Some("docx") => Ok(Self::Docx),
            Some("odt") => Ok(Self::Odt),
            Some(other) => bail!("unsupported attachment format: {other}"),
            None => bail!("attachment has no file extension"),
        }
    }
}

pub fn extract_text_from_path(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    match AttachmentFormat::from_path(path)? {
        AttachmentFormat::Pdf => extract_pdf_text(path),
        AttachmentFormat::Docx => extract_docx_text(path),
        AttachmentFormat::Odt => extract_odt_text(path),
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

fn extract_docx_text(path: &Path) -> Result<String> {
    let text = docx_lite::extract_text_from_bytes(
        &std::fs::read(path).with_context(|| format!("read DOCX at {}", path.display()))?,
    )
    .with_context(|| format!("extract DOCX text from {}", path.display()))?;

    Ok(normalize_whitespace(&text))
}

fn extract_odt_text(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("open ODT at {}", path.display()))?;
    let mut archive =
        ZipArchive::new(file).with_context(|| format!("read ZIP container {}", path.display()))?;
    let mut content_xml = String::new();

    archive
        .by_name("content.xml")
        .with_context(|| format!("read content.xml from {}", path.display()))?
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
                if matches!(local_name(end.name().as_ref()), b"p" | b"h" | b"list-item" | b"table-row")
                {
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
