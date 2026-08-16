use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

use super::super::*;

#[derive(Debug, Clone)]
pub(in crate::service) struct ConvertIdSource {
    id: String,
    format: Option<String>,
    element: &'static str,
}

#[derive(Debug, Clone)]
pub(in crate::service) struct CanonicalEwsObjectId {
    family: &'static str,
    id: String,
}

#[derive(Debug, Clone)]
pub(in crate::service) struct ConvertIdOutput {
    family: &'static str,
    format: &'static str,
    id: String,
}

pub(in crate::service) fn convert_id_success_response(alternate_ids: String) -> String {
    format!(
        concat!(
            "<m:ConvertIdResponse>",
            "<m:ResponseMessages>",
            "<m:ConvertIdResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{alternate_ids}",
            "</m:ConvertIdResponseMessage>",
            "</m:ResponseMessages>",
            "</m:ConvertIdResponse>"
        ),
        alternate_ids = alternate_ids,
    )
}

pub(in crate::service) fn convert_id_xml(output: &ConvertIdOutput) -> String {
    let element = match output.family {
        "public-folder" => "AlternatePublicFolderId",
        "public-folder-item" => "AlternatePublicFolderItemId",
        _ => "AlternateId",
    };
    format!(
        "<t:{element} Format=\"{format}\" Id=\"{id}\"/>",
        element = element,
        format = escape_xml(output.format),
        id = escape_xml(&output.id),
    )
}

pub(in crate::service) fn canonical_message_id_from_ews_id(id: &str) -> Option<Uuid> {
    id.strip_prefix("message:")
        .unwrap_or(id)
        .split(':')
        .next()
        .and_then(|value| Uuid::parse_str(value).ok())
}

/// [MS-OXWSCORE] section 2.2.4.25 defines ChangeKey as an identifier for a
/// specific item version.  The token deliberately contains only a
/// collision-resistant digest of the canonical identity and durable revision.
pub(in crate::service) fn versioned_change_key(kind: &str, id: &str, version: &str) -> String {
    let mut hasher = Sha256::new();
    for part in [kind, id, version] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    format!("ck-v1-{}", URL_SAFE_NO_PAD.encode(hasher.finalize()))
}

pub(in crate::service) fn requested_convert_ids(request: &str) -> Result<Vec<ConvertIdSource>> {
    let source_ids = element_content(request, "SourceIds")
        .ok_or_else(|| anyhow!("ConvertId requires SourceIds."))?;
    [
        "AlternateId",
        "AlternatePublicFolderId",
        "AlternatePublicFolderItemId",
    ]
    .into_iter()
    .map(|tag| convert_id_sources_for_tag(source_ids, tag))
    .collect::<Result<Vec<_>>>()
    .map(|sources| sources.into_iter().flatten().collect())
}

fn convert_id_sources_for_tag(
    request: &str,
    local_name: &'static str,
) -> Result<Vec<ConvertIdSource>> {
    let mut values = Vec::new();
    let mut rest = request;
    while let Some(tag_start) = rest.find('<') {
        let tag_text = rest[tag_start + 1..].trim_start();
        if tag_text.starts_with('/') || tag_text.starts_with('?') || tag_text.starts_with('!') {
            rest = &tag_text[1..];
            continue;
        }
        let Some(tag_end) = tag_text.find('>') else {
            break;
        };
        let open_tag = &tag_text[..tag_end];
        let Some(qualified_name) = open_tag
            .split(|value: char| value.is_whitespace() || value == '/')
            .next()
        else {
            break;
        };
        if qualified_name.rsplit(':').next() == Some(local_name) {
            let id = attribute_value(open_tag, "Id")
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow!("ConvertId {local_name} source is missing Id"))?;
            if id.len() > 512 {
                bail!("ConvertId source Id exceeds the supported 512-byte bound")
            }
            let format = attribute_value(open_tag, "Format")
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| anyhow!("ConvertId {local_name} source is missing Format"))?;
            values.push(ConvertIdSource {
                id: id.to_string(),
                format: Some(format.to_string()),
                element: local_name,
            });
        }
        rest = &tag_text[tag_end + 1..];
    }
    Ok(values)
}

pub(in crate::service) fn canonical_ews_object_id_from_convert_source(
    source: &ConvertIdSource,
) -> Result<CanonicalEwsObjectId> {
    let format = normalize_convert_id_format(
        source
            .format
            .as_deref()
            .ok_or_else(|| anyhow!("ConvertId source is missing Format"))?,
    )?;
    let id = if format == "HexEntryId" {
        decode_hex_entry_id(&source.id)?
    } else {
        source.id.clone()
    };
    if format == "EwsId" && id.starts_with("LPEEWS1.") {
        bail!("EwsId ConvertId sources must use the canonical LPE id shape")
    }
    if format != "EwsId" && !id.starts_with("LPEEWS1.") {
        bail!("alternate ConvertId sources must use the LPEEWS1 opaque id shape")
    }
    if let Some(payload) = id.strip_prefix("LPEEWS1.") {
        let decoded = URL_SAFE_NO_PAD
            .decode(payload.as_bytes())
            .map_err(|_| anyhow!("opaque ConvertId source is not valid base64url"))?;
        let decoded = String::from_utf8(decoded)
            .map_err(|_| anyhow!("opaque ConvertId source is not valid UTF-8"))?;
        let canonical = canonical_ews_object_id_from_payload(&decoded)?;
        validate_convert_source_element(source, &canonical)?;
        return Ok(canonical);
    }
    let canonical = canonical_ews_object_id_from_canonical_id(&id)?;
    validate_convert_source_element(source, &canonical)?;
    Ok(canonical)
}

/// [MS-OXWSCVTID] §3.1.4.1.3.5-.7: the public-folder alternate elements remain
/// distinct from generic alternate item ids.  This is syntax-only; ConvertId
/// deliberately does not reveal whether the canonical object exists.
fn validate_convert_source_element(
    source: &ConvertIdSource,
    canonical: &CanonicalEwsObjectId,
) -> Result<()> {
    let valid = match source.element {
        "AlternatePublicFolderId" => canonical.family == "public-folder",
        "AlternatePublicFolderItemId" => canonical.family == "public-folder-item",
        "AlternateId" => !matches!(canonical.family, "public-folder" | "public-folder-item"),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        bail!("ConvertId source element does not match the canonical id family")
    }
}

fn canonical_ews_object_id_from_payload(payload: &str) -> Result<CanonicalEwsObjectId> {
    let Some(rest) = payload.strip_prefix("lpeews:v1:") else {
        bail!("opaque ConvertId source has an unsupported version")
    };
    canonical_ews_object_id_from_canonical_id(rest)
}

fn canonical_ews_object_id_from_canonical_id(id: &str) -> Result<CanonicalEwsObjectId> {
    let (family, rest) = id
        .split_once(':')
        .ok_or_else(|| anyhow!("ConvertId source id is not a supported LPE EWS id"))?;
    match family {
        "message" | "mailbox" | "contact" | "event" | "task" | "public-folder"
        | "public-folder-item" => {
            Uuid::parse_str(rest)
                .map_err(|_| anyhow!("ConvertId source id has an invalid UUID"))?;
            Ok(CanonicalEwsObjectId {
                family: canonical_ews_family(family)?,
                id: id.to_string(),
            })
        }
        "attachment" => {
            let (message_id, attachment_id) = rest.split_once(':').ok_or_else(|| {
                anyhow!("attachment ConvertId source must include parent and attachment ids")
            })?;
            Uuid::parse_str(message_id)
                .map_err(|_| anyhow!("attachment ConvertId source has an invalid parent id"))?;
            Uuid::parse_str(attachment_id)
                .map_err(|_| anyhow!("attachment ConvertId source has an invalid attachment id"))?;
            Ok(CanonicalEwsObjectId {
                family: "attachment",
                id: id.to_string(),
            })
        }
        _ => bail!("ConvertId source id family `{family}` is not supported"),
    }
}

fn canonical_ews_family(family: &str) -> Result<&'static str> {
    match family {
        "message" => Ok("message"),
        "mailbox" => Ok("mailbox"),
        "contact" => Ok("contact"),
        "event" => Ok("event"),
        "task" => Ok("task"),
        "public-folder" => Ok("public-folder"),
        "public-folder-item" => Ok("public-folder-item"),
        _ => bail!("unsupported ConvertId family `{family}`"),
    }
}

pub(in crate::service) fn convert_canonical_ews_object_id(
    canonical: &CanonicalEwsObjectId,
    destination_format: &str,
) -> Result<ConvertIdOutput> {
    let format = normalize_convert_id_format(destination_format)?;
    let id = if format == "EwsId" {
        canonical.id.clone()
    } else {
        let opaque = opaque_ews_id(&canonical.id);
        if format == "HexEntryId" {
            encode_hex_entry_id(&opaque)
        } else {
            opaque
        }
    };
    Ok(ConvertIdOutput {
        family: canonical.family,
        format,
        id,
    })
}

fn normalize_convert_id_format(format: &str) -> Result<&'static str> {
    match format {
        value if value.eq_ignore_ascii_case("EwsId") => Ok("EwsId"),
        value if value.eq_ignore_ascii_case("EwsLegacyId") => Ok("EwsLegacyId"),
        value if value.eq_ignore_ascii_case("OwaId") => Ok("OwaId"),
        value if value.eq_ignore_ascii_case("EntryId") => Ok("EntryId"),
        value if value.eq_ignore_ascii_case("HexEntryId") => Ok("HexEntryId"),
        value if value.eq_ignore_ascii_case("StoreId") => Ok("StoreId"),
        _ => bail!("ConvertId destination format `{format}` is not supported"),
    }
}

fn opaque_ews_id(canonical_id: &str) -> String {
    format!(
        "LPEEWS1.{}",
        URL_SAFE_NO_PAD.encode(format!("lpeews:v1:{canonical_id}").as_bytes())
    )
}

fn encode_hex_entry_id(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect()
}

fn decode_hex_entry_id(value: &str) -> Result<String> {
    if value.len() % 2 != 0 {
        bail!("HexEntryId source has an odd number of characters");
    }
    let mut bytes = Vec::new();
    for index in (0..value.len()).step_by(2) {
        let byte = u8::from_str_radix(&value[index..index + 2], 16)
            .map_err(|_| anyhow!("HexEntryId source contains non-hex characters"))?;
        bytes.push(byte);
    }
    String::from_utf8(bytes).map_err(|_| anyhow!("HexEntryId source is not valid UTF-8"))
}

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn convert_id(&self, request: &str) -> Result<String> {
        let result = async {
            let destination_format =
                attribute_value_after(request, "ConvertId", "DestinationFormat")
                    .or_else(|| {
                        attribute_value_after(request, "ConvertIdRequest", "DestinationFormat")
                    })
                    .ok_or_else(|| anyhow!("ConvertId requires DestinationFormat."))?;
            let source_ids = requested_convert_ids(request)?;
            if source_ids.is_empty() {
                bail!("ConvertId requires at least one source id.");
            }

            let mut converted = String::new();
            for source in source_ids {
                let canonical = canonical_ews_object_id_from_convert_source(&source)?;
                let output = convert_canonical_ews_object_id(&canonical, destination_format)?;
                converted.push_str(&convert_id_xml(&output));
            }

            Ok(convert_id_success_response(converted))
        }
        .await;

        Ok(result.unwrap_or_else(|error: anyhow::Error| {
            operation_error_response("ConvertId", "ErrorInvalidId", &error.to_string())
        }))
    }
}
