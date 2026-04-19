use anyhow::{anyhow, bail, Result};

#[derive(Debug, Clone)]
pub(crate) struct WbxmlNode {
    pub(crate) page: u8,
    pub(crate) name: String,
    pub(crate) text: Option<String>,
    pub(crate) children: Vec<WbxmlNode>,
}

impl WbxmlNode {
    pub(crate) fn new(page: u8, name: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: None,
            children: Vec::new(),
        }
    }

    pub(crate) fn with_text(page: u8, name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: Some(text.into()),
            children: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, child: WbxmlNode) {
        self.children.push(child);
    }

    pub(crate) fn child(&self, name: &str) -> Option<&WbxmlNode> {
        self.children.iter().find(|child| child.name == name)
    }

    pub(crate) fn children_named(&self, name: &str) -> Vec<&WbxmlNode> {
        self.children
            .iter()
            .filter(|child| child.name == name)
            .collect()
    }

    pub(crate) fn text_value(&self) -> &str {
        self.text.as_deref().unwrap_or("")
    }
}

pub(crate) fn encode_wbxml(root: &WbxmlNode) -> Vec<u8> {
    let mut out = vec![0x03, 0x01, 0x6A, 0x00];
    let mut page = 0u8;
    encode_node(root, &mut page, &mut out);
    out
}

fn encode_node(node: &WbxmlNode, current_page: &mut u8, out: &mut Vec<u8>) {
    if node.page != *current_page {
        out.push(0x00);
        out.push(node.page);
        *current_page = node.page;
    }

    let token = token_for(node.page, &node.name).unwrap_or(0x05);
    let has_content = node.text.is_some() || !node.children.is_empty();
    out.push(if has_content { token | 0x40 } else { token });

    if let Some(text) = &node.text {
        out.push(0x03);
        out.extend_from_slice(text.as_bytes());
        out.push(0x00);
    }

    for child in &node.children {
        encode_node(child, current_page, out);
    }

    if has_content {
        out.push(0x01);
    }
}

pub(crate) fn decode_wbxml(bytes: &[u8]) -> Result<WbxmlNode> {
    let mut cursor = 0usize;
    if bytes.len() < 4 {
        bail!("WBXML payload is too short");
    }
    cursor += 1;
    let _ = read_multibyte_int(bytes, &mut cursor)?;
    let charset = read_multibyte_int(bytes, &mut cursor)?;
    if charset != 0x6A {
        bail!("unsupported WBXML charset");
    }
    let string_table_length = read_multibyte_int(bytes, &mut cursor)?;
    if string_table_length != 0 {
        bail!("WBXML string tables are not supported");
    }

    let mut current_page = 0u8;
    parse_node(bytes, &mut cursor, &mut current_page)
}

fn parse_node(bytes: &[u8], cursor: &mut usize, current_page: &mut u8) -> Result<WbxmlNode> {
    while *cursor < bytes.len() && bytes[*cursor] == 0x00 {
        *cursor += 1;
        *current_page = *bytes
            .get(*cursor)
            .ok_or_else(|| anyhow!("missing WBXML code page"))?;
        *cursor += 1;
    }

    let token = *bytes
        .get(*cursor)
        .ok_or_else(|| anyhow!("missing WBXML token"))?;
    *cursor += 1;
    if token == 0x01 {
        bail!("unexpected WBXML end token");
    }

    if token & 0x80 != 0 {
        bail!("WBXML attributes are not supported");
    }
    let has_content = token & 0x40 != 0;
    let name =
        name_for(*current_page, token & 0x3F).ok_or_else(|| anyhow!("unknown WBXML token"))?;
    let mut node = WbxmlNode::new(*current_page, name);

    if has_content {
        let mut text = String::new();
        while *cursor < bytes.len() {
            match bytes[*cursor] {
                0x00 => {
                    *cursor += 1;
                    *current_page = *bytes
                        .get(*cursor)
                        .ok_or_else(|| anyhow!("missing WBXML code page"))?;
                    *cursor += 1;
                }
                0x01 => {
                    *cursor += 1;
                    break;
                }
                0x03 => {
                    *cursor += 1;
                    text.push_str(&read_inline_string(bytes, cursor)?);
                }
                0xC3 => {
                    *cursor += 1;
                    let length = read_multibyte_int(bytes, cursor)? as usize;
                    let chunk = bytes
                        .get(*cursor..*cursor + length)
                        .ok_or_else(|| anyhow!("invalid WBXML opaque block"))?;
                    text.push_str(&String::from_utf8_lossy(chunk));
                    *cursor += length;
                }
                _ => node.children.push(parse_node(bytes, cursor, current_page)?),
            }
        }
        if !text.is_empty() {
            node.text = Some(text);
        }
    }

    Ok(node)
}

fn read_multibyte_int(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| anyhow!("unexpected end of WBXML payload"))?;
        *cursor += 1;
        value = (value << 7) | (byte & 0x7F) as u32;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
}

fn read_inline_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    let start = *cursor;
    while *cursor < bytes.len() && bytes[*cursor] != 0x00 {
        *cursor += 1;
    }
    let value = String::from_utf8(bytes[start..*cursor].to_vec())?;
    *cursor += 1;
    Ok(value)
}

fn token_for(page: u8, name: &str) -> Option<u8> {
    match (page, name) {
        (0, "Sync") => Some(0x05),
        (0, "Responses") => Some(0x06),
        (0, "Add") => Some(0x07),
        (0, "Change") => Some(0x08),
        (0, "Delete") => Some(0x09),
        (0, "Fetch") => Some(0x0A),
        (0, "SyncKey") => Some(0x0B),
        (0, "ClientId") => Some(0x0C),
        (0, "ServerId") => Some(0x0D),
        (0, "Status") => Some(0x0E),
        (0, "Collection") => Some(0x0F),
        (0, "Class") => Some(0x10),
        (0, "CollectionId") => Some(0x12),
        (0, "GetChanges") => Some(0x13),
        (0, "MoreAvailable") => Some(0x14),
        (0, "WindowSize") => Some(0x15),
        (0, "Commands") => Some(0x16),
        (0, "Options") => Some(0x17),
        (0, "FilterType") => Some(0x18),
        (0, "Conflict") => Some(0x1B),
        (0, "Collections") => Some(0x1C),
        (0, "ApplicationData") => Some(0x1D),
        (0, "DeletesAsMoves") => Some(0x1E),
        (0, "Supported") => Some(0x20),
        (0, "SoftDelete") => Some(0x21),
        (0, "MIMESupport") => Some(0x22),
        (0, "MIMETruncation") => Some(0x23),
        (0, "Wait") => Some(0x24),
        (0, "Limit") => Some(0x25),
        (0, "Partial") => Some(0x26),
        (0, "ConversationMode") => Some(0x27),
        (0, "MaxItems") => Some(0x28),
        (0, "HeartbeatInterval") => Some(0x29),
        (1, "Email1Address") => Some(0x1B),
        (1, "FileAs") => Some(0x1E),
        (1, "FirstName") => Some(0x1F),
        (1, "HomePhoneNumber") => Some(0x27),
        (1, "LastName") => Some(0x29),
        (1, "MobilePhoneNumber") => Some(0x2B),
        (2, "DateReceived") => Some(0x0F),
        (2, "DisplayTo") => Some(0x11),
        (2, "Importance") => Some(0x12),
        (2, "MessageClass") => Some(0x13),
        (2, "Subject") => Some(0x14),
        (2, "Read") => Some(0x15),
        (2, "To") => Some(0x16),
        (2, "Cc") => Some(0x17),
        (2, "From") => Some(0x18),
        (2, "ReplyTo") => Some(0x19),
        (4, "Attendees") => Some(0x07),
        (4, "Attendee") => Some(0x08),
        (4, "Email") => Some(0x09),
        (4, "Name") => Some(0x0A),
        (4, "EndTime") => Some(0x12),
        (4, "Location") => Some(0x17),
        (4, "OrganizerEmail") => Some(0x19),
        (4, "OrganizerName") => Some(0x1A),
        (4, "Reminder") => Some(0x24),
        (4, "Subject") => Some(0x26),
        (4, "StartTime") => Some(0x27),
        (7, "DisplayName") => Some(0x07),
        (7, "ServerId") => Some(0x08),
        (7, "ParentId") => Some(0x09),
        (7, "Type") => Some(0x0A),
        (7, "Status") => Some(0x0C),
        (7, "Changes") => Some(0x0E),
        (7, "Add") => Some(0x0F),
        (7, "Delete") => Some(0x10),
        (7, "Update") => Some(0x11),
        (7, "SyncKey") => Some(0x12),
        (7, "FolderSync") => Some(0x16),
        (7, "Count") => Some(0x17),
        (14, "Provision") => Some(0x05),
        (14, "Policies") => Some(0x06),
        (14, "Policy") => Some(0x07),
        (14, "PolicyType") => Some(0x08),
        (14, "PolicyKey") => Some(0x09),
        (14, "Data") => Some(0x0A),
        (14, "Status") => Some(0x0B),
        (14, "EASProvisionDoc") => Some(0x0D),
        (14, "DevicePasswordEnabled") => Some(0x0E),
        (14, "AlphanumericDevicePasswordRequired") => Some(0x0F),
        (14, "AttachmentsEnabled") => Some(0x13),
        (14, "MinDevicePasswordLength") => Some(0x14),
        (14, "AllowSimpleDevicePassword") => Some(0x18),
        (14, "AllowStorageCard") => Some(0x1B),
        (14, "AllowCamera") => Some(0x1C),
        (14, "RequireDeviceEncryption") => Some(0x1D),
        (14, "AllowWiFi") => Some(0x21),
        (14, "AllowTextMessaging") => Some(0x22),
        (14, "AllowPOPIMAPEmail") => Some(0x23),
        (14, "AllowBrowser") => Some(0x33),
        (14, "AllowConsumerEmail") => Some(0x34),
        (17, "BodyPreference") => Some(0x05),
        (17, "Type") => Some(0x06),
        (17, "TruncationSize") => Some(0x07),
        (17, "AllOrNone") => Some(0x08),
        (17, "Body") => Some(0x0A),
        (17, "Data") => Some(0x0B),
        (17, "EstimatedDataSize") => Some(0x0C),
        (17, "Truncated") => Some(0x0D),
        (18, "Status") => Some(0x06),
        (18, "Set") => Some(0x08),
        (18, "DeviceInformation") => Some(0x16),
        (18, "Model") => Some(0x17),
        (18, "IMEI") => Some(0x18),
        (18, "FriendlyName") => Some(0x19),
        (18, "OS") => Some(0x1A),
        (18, "OSLanguage") => Some(0x1B),
        (18, "PhoneNumber") => Some(0x1C),
        (18, "UserAgent") => Some(0x20),
        (18, "MobileOperator") => Some(0x22),
        (21, "SendMail") => Some(0x05),
        (21, "SaveInSentItems") => Some(0x08),
        (21, "Mime") => Some(0x10),
        (21, "ClientId") => Some(0x11),
        (21, "Status") => Some(0x12),
        _ => None,
    }
}

fn name_for(page: u8, token: u8) -> Option<&'static str> {
    match (page, token) {
        (0, 0x05) => Some("Sync"),
        (0, 0x06) => Some("Responses"),
        (0, 0x07) => Some("Add"),
        (0, 0x08) => Some("Change"),
        (0, 0x09) => Some("Delete"),
        (0, 0x0A) => Some("Fetch"),
        (0, 0x0B) => Some("SyncKey"),
        (0, 0x0C) => Some("ClientId"),
        (0, 0x0D) => Some("ServerId"),
        (0, 0x0E) => Some("Status"),
        (0, 0x0F) => Some("Collection"),
        (0, 0x10) => Some("Class"),
        (0, 0x12) => Some("CollectionId"),
        (0, 0x13) => Some("GetChanges"),
        (0, 0x14) => Some("MoreAvailable"),
        (0, 0x15) => Some("WindowSize"),
        (0, 0x16) => Some("Commands"),
        (0, 0x17) => Some("Options"),
        (0, 0x18) => Some("FilterType"),
        (0, 0x1B) => Some("Conflict"),
        (0, 0x1C) => Some("Collections"),
        (0, 0x1D) => Some("ApplicationData"),
        (0, 0x1E) => Some("DeletesAsMoves"),
        (0, 0x20) => Some("Supported"),
        (0, 0x21) => Some("SoftDelete"),
        (0, 0x22) => Some("MIMESupport"),
        (0, 0x23) => Some("MIMETruncation"),
        (0, 0x24) => Some("Wait"),
        (0, 0x25) => Some("Limit"),
        (0, 0x26) => Some("Partial"),
        (0, 0x27) => Some("ConversationMode"),
        (0, 0x28) => Some("MaxItems"),
        (0, 0x29) => Some("HeartbeatInterval"),
        (1, 0x1B) => Some("Email1Address"),
        (1, 0x1E) => Some("FileAs"),
        (1, 0x1F) => Some("FirstName"),
        (1, 0x27) => Some("HomePhoneNumber"),
        (1, 0x29) => Some("LastName"),
        (1, 0x2B) => Some("MobilePhoneNumber"),
        (2, 0x0F) => Some("DateReceived"),
        (2, 0x11) => Some("DisplayTo"),
        (2, 0x12) => Some("Importance"),
        (2, 0x13) => Some("MessageClass"),
        (2, 0x14) => Some("Subject"),
        (2, 0x15) => Some("Read"),
        (2, 0x16) => Some("To"),
        (2, 0x17) => Some("Cc"),
        (2, 0x18) => Some("From"),
        (2, 0x19) => Some("ReplyTo"),
        (4, 0x07) => Some("Attendees"),
        (4, 0x08) => Some("Attendee"),
        (4, 0x09) => Some("Email"),
        (4, 0x0A) => Some("Name"),
        (4, 0x12) => Some("EndTime"),
        (4, 0x17) => Some("Location"),
        (4, 0x19) => Some("OrganizerEmail"),
        (4, 0x1A) => Some("OrganizerName"),
        (4, 0x24) => Some("Reminder"),
        (4, 0x26) => Some("Subject"),
        (4, 0x27) => Some("StartTime"),
        (7, 0x07) => Some("DisplayName"),
        (7, 0x08) => Some("ServerId"),
        (7, 0x09) => Some("ParentId"),
        (7, 0x0A) => Some("Type"),
        (7, 0x0C) => Some("Status"),
        (7, 0x0E) => Some("Changes"),
        (7, 0x0F) => Some("Add"),
        (7, 0x10) => Some("Delete"),
        (7, 0x11) => Some("Update"),
        (7, 0x12) => Some("SyncKey"),
        (7, 0x16) => Some("FolderSync"),
        (7, 0x17) => Some("Count"),
        (14, 0x05) => Some("Provision"),
        (14, 0x06) => Some("Policies"),
        (14, 0x07) => Some("Policy"),
        (14, 0x08) => Some("PolicyType"),
        (14, 0x09) => Some("PolicyKey"),
        (14, 0x0A) => Some("Data"),
        (14, 0x0B) => Some("Status"),
        (14, 0x0D) => Some("EASProvisionDoc"),
        (14, 0x0E) => Some("DevicePasswordEnabled"),
        (14, 0x0F) => Some("AlphanumericDevicePasswordRequired"),
        (14, 0x13) => Some("AttachmentsEnabled"),
        (14, 0x14) => Some("MinDevicePasswordLength"),
        (14, 0x18) => Some("AllowSimpleDevicePassword"),
        (14, 0x1B) => Some("AllowStorageCard"),
        (14, 0x1C) => Some("AllowCamera"),
        (14, 0x1D) => Some("RequireDeviceEncryption"),
        (14, 0x21) => Some("AllowWiFi"),
        (14, 0x22) => Some("AllowTextMessaging"),
        (14, 0x23) => Some("AllowPOPIMAPEmail"),
        (14, 0x33) => Some("AllowBrowser"),
        (14, 0x34) => Some("AllowConsumerEmail"),
        (17, 0x05) => Some("BodyPreference"),
        (17, 0x06) => Some("Type"),
        (17, 0x07) => Some("TruncationSize"),
        (17, 0x08) => Some("AllOrNone"),
        (17, 0x0A) => Some("Body"),
        (17, 0x0B) => Some("Data"),
        (17, 0x0C) => Some("EstimatedDataSize"),
        (17, 0x0D) => Some("Truncated"),
        (18, 0x06) => Some("Status"),
        (18, 0x08) => Some("Set"),
        (18, 0x16) => Some("DeviceInformation"),
        (18, 0x17) => Some("Model"),
        (18, 0x18) => Some("IMEI"),
        (18, 0x19) => Some("FriendlyName"),
        (18, 0x1A) => Some("OS"),
        (18, 0x1B) => Some("OSLanguage"),
        (18, 0x1C) => Some("PhoneNumber"),
        (18, 0x20) => Some("UserAgent"),
        (18, 0x22) => Some("MobileOperator"),
        (21, 0x05) => Some("SendMail"),
        (21, 0x08) => Some("SaveInSentItems"),
        (21, 0x10) => Some("Mime"),
        (21, 0x11) => Some("ClientId"),
        (21, 0x12) => Some("Status"),
        _ => None,
    }
}
