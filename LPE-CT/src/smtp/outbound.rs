use lpe_domain::OutboundMessageHandoffRequest;

pub(crate) fn compose_rfc822_message(payload: &OutboundMessageHandoffRequest) -> Vec<u8> {
    let mut lines = Vec::new();
    lines.push(format!(
        "From: {}",
        format_address(&payload.from_address, payload.from_display.as_deref())
    ));
    if let Some(sender_address) = payload
        .sender_address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case(&payload.from_address))
    {
        lines.push(format!(
            "Sender: {}",
            format_address(sender_address, payload.sender_display.as_deref())
        ));
    }
    if !payload.to.is_empty() {
        lines.push(format!(
            "To: {}",
            payload
                .to
                .iter()
                .map(|recipient| format_address(
                    &recipient.address,
                    recipient.display_name.as_deref()
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !payload.cc.is_empty() {
        lines.push(format!(
            "Cc: {}",
            payload
                .cc
                .iter()
                .map(|recipient| format_address(
                    &recipient.address,
                    recipient.display_name.as_deref()
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    lines.push(format!("Subject: {}", payload.subject));
    lines.push(format!(
        "Message-Id: {}",
        payload
            .internet_message_id
            .clone()
            .unwrap_or_else(|| format!("<{}@lpe.local>", payload.message_id))
    ));
    lines.push("MIME-Version: 1.0".to_string());
    if let Some(html) = payload
        .body_html_sanitized
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let boundary = format!("lpe-alt-{}", payload.message_id);
        lines.push(format!(
            "Content-Type: multipart/alternative; boundary=\"{boundary}\""
        ));
        lines.push(String::new());
        lines.push(format!("--{boundary}"));
        lines.push("Content-Type: text/plain; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(&payload.body_text));
        lines.push(format!("--{boundary}"));
        lines.push("Content-Type: text/html; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(html));
        lines.push(format!("--{boundary}--"));
    } else {
        lines.push("Content-Type: text/plain; charset=utf-8".to_string());
        lines.push("Content-Transfer-Encoding: quoted-printable".to_string());
        lines.push(String::new());
        lines.push(encode_quoted_printable(&payload.body_text));
    }
    lines.join("\r\n").into_bytes()
}

fn format_address(address: &str, display_name: Option<&str>) -> String {
    match display_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(display_name) => format!("{display_name} <{address}>"),
        None => address.to_string(),
    }
}

pub(crate) fn encode_quoted_printable(value: &str) -> String {
    let mut encoded = String::new();
    let mut line_len = 0usize;
    for &byte in value.as_bytes() {
        match byte {
            b'\r' => {}
            b'\n' => {
                encoded.push_str("\r\n");
                line_len = 0;
            }
            b'\t' | b' ' | 33..=60 | 62..=126 => {
                if line_len >= 72 {
                    encoded.push_str("=\r\n");
                    line_len = 0;
                }
                encoded.push(byte as char);
                line_len += 1;
            }
            _ => {
                if line_len >= 70 {
                    encoded.push_str("=\r\n");
                    line_len = 0;
                }
                encoded.push_str(&format!("={byte:02X}"));
                line_len += 3;
            }
        }
    }
    encoded
}
