use super::*;

#[derive(Debug, Clone, Copy)]
pub(crate) enum SmtpPathKind {
    MailFrom,
    RcptTo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSmtpPath {
    pub(crate) address: String,
    pub(crate) declared_size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SmtpPathError {
    MalformedPath,
    InvalidAddress,
    InvalidSize,
    SizeTooLarge,
    UnsupportedParameter(String),
}

#[derive(Debug, Clone)]
pub(in crate::smtp) struct SmtpReply {
    pub(in crate::smtp) code: u16,
    pub(in crate::smtp) message: String,
}

pub(in crate::smtp) async fn smtp_command(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    command: &str,
    expected: u16,
) -> Result<()> {
    writer.write_all(command.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    expect_smtp(reader, expected).await
}

pub(in crate::smtp) async fn smtp_command_reply(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    command: &str,
) -> Result<SmtpReply> {
    writer.write_all(command.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    read_smtp_reply(reader).await
}

pub(in crate::smtp) async fn expect_smtp(
    reader: &mut BufReader<OwnedReadHalf>,
    expected: u16,
) -> Result<()> {
    let reply = read_smtp_reply(reader).await?;
    if reply.code == expected {
        Ok(())
    } else {
        Err(anyhow!("unexpected SMTP response: {}", reply.message))
    }
}

pub(in crate::smtp) async fn read_smtp_reply(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<SmtpReply> {
    let mut line = String::new();
    let mut message = String::new();
    let code = loop {
        line.clear();
        reader.read_line(&mut line).await?;
        if line.len() < 3 {
            return Err(anyhow!("invalid SMTP response"));
        }
        let code = line[0..3].parse::<u16>().unwrap_or(0);
        let more = line.as_bytes().get(3) == Some(&b'-');
        let trimmed = line.trim_end().to_string();
        if !message.is_empty() {
            message.push('\n');
        }
        message.push_str(&trimmed);
        if !more {
            break code;
        }
    };

    Ok(SmtpReply { code, message })
}

pub(in crate::smtp) async fn read_smtp_data<R>(reader: &mut R, max_mb: u32) -> Result<Vec<u8>>
where
    R: AsyncBufRead + Unpin,
{
    let max_bytes = max_mb.max(1) as usize * 1024 * 1024;
    let mut data = Vec::new();
    let mut line = Vec::new();
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line).await? == 0 {
            return Err(anyhow!("client closed during DATA"));
        }
        if line == b".\r\n" || line == b".\n" {
            break;
        }
        if line.starts_with(b"..") {
            data.extend_from_slice(&line[1..]);
        } else {
            data.extend_from_slice(&line);
        }
        if data.len() > max_bytes {
            return Err(anyhow!("message exceeds configured maximum size"));
        }
    }
    Ok(data)
}

pub(crate) fn max_smtp_message_size_bytes(max_mb: u32) -> u64 {
    u64::from(max_mb.max(1)) * 1024 * 1024
}

pub(crate) fn parse_smtp_path(
    value: &str,
    kind: SmtpPathKind,
    max_message_size_bytes: u64,
) -> std::result::Result<ParsedSmtpPath, SmtpPathError> {
    let trimmed = value.trim();
    let Some(rest) = trimmed.strip_prefix('<') else {
        return Err(SmtpPathError::MalformedPath);
    };
    let Some(end) = rest.find('>') else {
        return Err(SmtpPathError::MalformedPath);
    };
    let path = &rest[..end];
    if path.contains(['<', '>']) {
        return Err(SmtpPathError::MalformedPath);
    }

    match kind {
        SmtpPathKind::MailFrom if path.is_empty() => {}
        SmtpPathKind::MailFrom | SmtpPathKind::RcptTo => {
            if !is_valid_smtp_mailbox(path) {
                return Err(SmtpPathError::InvalidAddress);
            }
        }
    }
    if matches!(kind, SmtpPathKind::RcptTo) && path.is_empty() {
        return Err(SmtpPathError::InvalidAddress);
    }

    let mut declared_size = None;
    for parameter in rest[end + 1..].split_ascii_whitespace() {
        let (name, value) = parameter
            .split_once('=')
            .map(|(name, value)| (name.to_ascii_uppercase(), Some(value)))
            .unwrap_or_else(|| (parameter.to_ascii_uppercase(), None));

        match (kind, name.as_str(), value) {
            (SmtpPathKind::MailFrom, "SIZE", Some(size)) => {
                if declared_size.is_some() || size.is_empty() {
                    return Err(SmtpPathError::InvalidSize);
                }
                let parsed = size
                    .parse::<u64>()
                    .map_err(|_| SmtpPathError::SizeTooLarge)?;
                if parsed > max_message_size_bytes {
                    return Err(SmtpPathError::SizeTooLarge);
                }
                declared_size = Some(parsed);
            }
            (SmtpPathKind::MailFrom, "SIZE", None) => {
                return Err(SmtpPathError::InvalidSize);
            }
            _ => return Err(SmtpPathError::UnsupportedParameter(name)),
        }
    }

    Ok(ParsedSmtpPath {
        address: path.to_ascii_lowercase(),
        declared_size,
    })
}

fn is_valid_smtp_mailbox(address: &str) -> bool {
    if address.is_empty()
        || address.len() > 254
        || address
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || address.contains(['<', '>', ',', ';'])
    {
        return false;
    }

    let Some((local, domain)) = address.rsplit_once('@') else {
        return false;
    };
    if local.is_empty()
        || local.len() > 64
        || local.starts_with('.')
        || local.ends_with('.')
        || local.contains("..")
        || domain.is_empty()
        || domain.len() > 253
        || domain.starts_with('.')
        || domain.ends_with('.')
    {
        return false;
    }

    domain.split('.').all(|label| {
        !label.is_empty()
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    })
}

pub(crate) fn smtp_path_error_reply(command: &str, error: SmtpPathError) -> String {
    match error {
        SmtpPathError::MalformedPath | SmtpPathError::InvalidAddress => {
            format!("501 malformed {command} path")
        }
        SmtpPathError::InvalidSize => "501 invalid SIZE parameter".to_string(),
        SmtpPathError::SizeTooLarge => "552 message size exceeds configured maximum".to_string(),
        SmtpPathError::UnsupportedParameter(parameter) => {
            format!("555 {command} parameter not supported ({parameter})")
        }
    }
}

pub(in crate::smtp) async fn write_smtp<W>(writer: &mut W, line: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut response = Vec::with_capacity(line.len() + 2);
    response.extend_from_slice(line.as_bytes());
    response.extend_from_slice(b"\r\n");
    writer.write_all(&response).await?;
    writer.flush().await?;
    Ok(())
}
