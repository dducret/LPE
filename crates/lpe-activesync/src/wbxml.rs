use anyhow::{anyhow, bail, Result};

use crate::protocol::WbxmlCodePage;

#[derive(Debug, Clone)]
pub(crate) struct WbxmlNode {
    pub(crate) page: u8,
    pub(crate) name: String,
    pub(crate) text: Option<String>,
    pub(crate) opaque: Option<Vec<u8>>,
    pub(crate) children: Vec<WbxmlNode>,
}

impl WbxmlNode {
    pub(crate) fn new(page: u8, name: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: None,
            opaque: None,
            children: Vec::new(),
        }
    }

    pub(crate) fn with_text(page: u8, name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            page,
            name: name.into(),
            text: Some(text.into()),
            opaque: None,
            children: Vec::new(),
        }
    }

    pub(crate) fn with_opaque(page: u8, name: impl Into<String>, data: Vec<u8>) -> Self {
        Self {
            page,
            name: name.into(),
            text: None,
            opaque: Some(data),
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

    let token = WbxmlCodePage::try_from(node.page)
        .ok()
        .and_then(|page| token_for(page, &node.name))
        .unwrap_or(0x05);
    let has_content = node.text.is_some() || node.opaque.is_some() || !node.children.is_empty();
    out.push(if has_content { token | 0x40 } else { token });

    if let Some(text) = &node.text {
        out.push(0x03);
        out.extend_from_slice(text.as_bytes());
        out.push(0x00);
    }

    if let Some(opaque) = &node.opaque {
        out.push(0xC3);
        write_multibyte_int(opaque.len() as u32, out);
        out.extend_from_slice(opaque);
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
        if WbxmlCodePage::try_from(*current_page).is_err() {
            tracing::warn!(
                adapter = "activesync",
                enum_name = "WbxmlCodePage",
                raw_value = *current_page,
                "unsupported WBXML code page; preserving raw node boundary"
            );
        }
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
    let token_value = token & 0x3F;
    let name = match WbxmlCodePage::try_from(*current_page)
        .ok()
        .and_then(|code_page| name_for(code_page, token_value))
    {
        Some(name) => name.to_string(),
        None => {
            tracing::warn!(
                adapter = "activesync",
                enum_name = "WbxmlToken",
                code_page = *current_page,
                raw_value = token_value,
                "unsupported WBXML token; preserving raw node boundary"
            );
            format!("UnsupportedWbxmlToken{current_page:02X}{token_value:02X}")
        }
    };
    let mut node = WbxmlNode::new(*current_page, name);

    if has_content {
        let mut text = String::new();
        let mut opaque = Vec::new();
        while *cursor < bytes.len() {
            match bytes[*cursor] {
                0x00 => {
                    *cursor += 1;
                    *current_page = *bytes
                        .get(*cursor)
                        .ok_or_else(|| anyhow!("missing WBXML code page"))?;
                    if WbxmlCodePage::try_from(*current_page).is_err() {
                        tracing::warn!(
                            adapter = "activesync",
                            enum_name = "WbxmlCodePage",
                            raw_value = *current_page,
                            "unsupported WBXML code page; preserving raw node boundary"
                        );
                    }
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
                    opaque.extend_from_slice(chunk);
                    *cursor += length;
                }
                _ => node.children.push(parse_node(bytes, cursor, current_page)?),
            }
        }
        if !text.is_empty() {
            node.text = Some(text);
        }
        if !opaque.is_empty() {
            node.opaque = Some(opaque);
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

fn write_multibyte_int(mut value: u32, out: &mut Vec<u8>) {
    let mut bytes = vec![(value & 0x7F) as u8];
    value >>= 7;
    while value > 0 {
        bytes.push(((value & 0x7F) as u8) | 0x80);
        value >>= 7;
    }
    bytes.reverse();
    out.extend_from_slice(&bytes);
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

fn token_for(page: WbxmlCodePage, name: &str) -> Option<u8> {
    use WbxmlCodePage::*;
    match (page, name) {
        (AirSync, "Sync") => Some(0x05),
        (AirSync, "Responses") => Some(0x06),
        (AirSync, "Add") => Some(0x07),
        (AirSync, "Change") => Some(0x08),
        (AirSync, "Delete") => Some(0x09),
        (AirSync, "Fetch") => Some(0x0A),
        (AirSync, "SyncKey") => Some(0x0B),
        (AirSync, "ClientId") => Some(0x0C),
        (AirSync, "ServerId") => Some(0x0D),
        (AirSync, "Status") => Some(0x0E),
        (AirSync, "Collection") => Some(0x0F),
        (AirSync, "Class") => Some(0x10),
        (AirSync, "CollectionId") => Some(0x12),
        (AirSync, "GetChanges") => Some(0x13),
        (AirSync, "MoreAvailable") => Some(0x14),
        (AirSync, "WindowSize") => Some(0x15),
        (AirSync, "Commands") => Some(0x16),
        (AirSync, "Options") => Some(0x17),
        (AirSync, "FilterType") => Some(0x18),
        (AirSync, "Conflict") => Some(0x1B),
        (AirSync, "Collections") => Some(0x1C),
        (AirSync, "ApplicationData") => Some(0x1D),
        (AirSync, "DeletesAsMoves") => Some(0x1E),
        (AirSync, "Supported") => Some(0x20),
        (AirSync, "SoftDelete") => Some(0x21),
        (AirSync, "MIMESupport") => Some(0x22),
        (AirSync, "MIMETruncation") => Some(0x23),
        (AirSync, "Wait") => Some(0x24),
        (AirSync, "Limit") => Some(0x25),
        (AirSync, "Partial") => Some(0x26),
        (AirSync, "ConversationMode") => Some(0x27),
        (AirSync, "MaxItems") => Some(0x28),
        (AirSync, "HeartbeatInterval") => Some(0x29),
        (Contacts, "Body") => Some(0x09),
        (Contacts, "BusinessPhoneNumber") => Some(0x13),
        (Contacts, "CompanyName") => Some(0x19),
        (Contacts, "Email1Address") => Some(0x1B),
        (Contacts, "FileAs") => Some(0x1E),
        (Contacts, "FirstName") => Some(0x1F),
        (Contacts, "HomePhoneNumber") => Some(0x27),
        (Contacts, "JobTitle") => Some(0x28),
        (Contacts, "LastName") => Some(0x29),
        (Contacts, "MobilePhoneNumber") => Some(0x2B),
        (Calendar, "TimeZone") => Some(0x05),
        (Calendar, "AllDayEvent") => Some(0x06),
        (Email, "DateReceived") => Some(0x0F),
        (Email, "DisplayTo") => Some(0x11),
        (Email, "Importance") => Some(0x12),
        (Email, "MessageClass") => Some(0x13),
        (Email, "Subject") => Some(0x14),
        (Email, "Read") => Some(0x15),
        (Email, "To") => Some(0x16),
        (Email, "Cc") => Some(0x17),
        (Email, "From") => Some(0x18),
        (Email, "ReplyTo") => Some(0x19),
        (Email, "Flag") => Some(0x3A),
        (Email, "Status") => Some(0x3B),
        (Email, "FlagType") => Some(0x3D),
        (Email, "CompleteTime") => Some(0x3E),
        (Calendar, "Attendees") => Some(0x07),
        (Calendar, "Attendee") => Some(0x08),
        (Calendar, "Email") => Some(0x09),
        (Calendar, "Name") => Some(0x0A),
        (Calendar, "BusyStatus") => Some(0x0D),
        (Calendar, "EndTime") => Some(0x12),
        (Calendar, "Location") => Some(0x17),
        (Calendar, "MeetingStatus") => Some(0x18),
        (Calendar, "OrganizerEmail") => Some(0x19),
        (Calendar, "OrganizerName") => Some(0x1A),
        (Calendar, "Recurrence") => Some(0x1B),
        (Calendar, "Type") => Some(0x1C),
        (Calendar, "Until") => Some(0x1D),
        (Calendar, "Occurrences") => Some(0x1E),
        (Calendar, "Interval") => Some(0x1F),
        (Calendar, "DayOfWeek") => Some(0x20),
        (Calendar, "DayOfMonth") => Some(0x21),
        (Calendar, "WeekOfMonth") => Some(0x22),
        (Calendar, "MonthOfYear") => Some(0x23),
        (Calendar, "Reminder") => Some(0x24),
        (Calendar, "Subject") => Some(0x26),
        (Calendar, "StartTime") => Some(0x27),
        (Calendar, "UID") => Some(0x28),
        (Calendar, "AttendeeStatus") => Some(0x29),
        (Calendar, "AttendeeType") => Some(0x2A),
        (Tasks, "DateCompleted") => Some(0x0B),
        (Tasks, "DueDate") => Some(0x0C),
        (Tasks, "UtcDueDate") => Some(0x0D),
        (Tasks, "StartDate") => Some(0x1E),
        (Tasks, "UtcStartDate") => Some(0x1F),
        (Move, "MoveItems") => Some(0x05),
        (Move, "Move") => Some(0x06),
        (Move, "SrcMsgId") => Some(0x07),
        (Move, "SrcFldId") => Some(0x08),
        (Move, "DstFldId") => Some(0x09),
        (Move, "Response") => Some(0x0A),
        (Move, "Status") => Some(0x0B),
        (Move, "DstMsgId") => Some(0x0C),
        (GetItemEstimate, "GetItemEstimate") => Some(0x05),
        (GetItemEstimate, "Collections") => Some(0x07),
        (GetItemEstimate, "Collection") => Some(0x08),
        (GetItemEstimate, "Class") => Some(0x09),
        (GetItemEstimate, "CollectionId") => Some(0x0A),
        (GetItemEstimate, "Estimate") => Some(0x0C),
        (GetItemEstimate, "Response") => Some(0x0D),
        (GetItemEstimate, "Status") => Some(0x0E),
        (FolderHierarchy, "DisplayName") => Some(0x07),
        (FolderHierarchy, "ServerId") => Some(0x08),
        (FolderHierarchy, "ParentId") => Some(0x09),
        (FolderHierarchy, "Type") => Some(0x0A),
        (FolderHierarchy, "Status") => Some(0x0C),
        (FolderHierarchy, "Changes") => Some(0x0E),
        (FolderHierarchy, "Add") => Some(0x0F),
        (FolderHierarchy, "Delete") => Some(0x10),
        (FolderHierarchy, "Update") => Some(0x11),
        (FolderHierarchy, "SyncKey") => Some(0x12),
        (FolderHierarchy, "FolderCreate") => Some(0x13),
        (FolderHierarchy, "FolderDelete") => Some(0x14),
        (FolderHierarchy, "FolderUpdate") => Some(0x15),
        (FolderHierarchy, "FolderSync") => Some(0x16),
        (FolderHierarchy, "Count") => Some(0x17),
        (Ping, "Ping") => Some(0x05),
        (Ping, "Status") => Some(0x07),
        (Ping, "HeartbeatInterval") => Some(0x08),
        (Ping, "Folders") => Some(0x09),
        (Ping, "Folder") => Some(0x0A),
        (Ping, "Id") => Some(0x0B),
        (Ping, "Class") => Some(0x0C),
        (Ping, "MaxFolders") => Some(0x0D),
        (Provision, "Provision") => Some(0x05),
        (Provision, "Policies") => Some(0x06),
        (Provision, "Policy") => Some(0x07),
        (Provision, "PolicyType") => Some(0x08),
        (Provision, "PolicyKey") => Some(0x09),
        (Provision, "Data") => Some(0x0A),
        (Provision, "Status") => Some(0x0B),
        (Provision, "EASProvisionDoc") => Some(0x0D),
        (Provision, "DevicePasswordEnabled") => Some(0x0E),
        (Provision, "AlphanumericDevicePasswordRequired") => Some(0x0F),
        (Provision, "AttachmentsEnabled") => Some(0x13),
        (Provision, "MinDevicePasswordLength") => Some(0x14),
        (Provision, "AllowSimpleDevicePassword") => Some(0x18),
        (Provision, "AllowStorageCard") => Some(0x1B),
        (Provision, "AllowCamera") => Some(0x1C),
        (Provision, "RequireDeviceEncryption") => Some(0x1D),
        (Provision, "AllowWiFi") => Some(0x21),
        (Provision, "AllowTextMessaging") => Some(0x22),
        (Provision, "AllowPOPIMAPEmail") => Some(0x23),
        (Provision, "AllowBrowser") => Some(0x33),
        (Provision, "AllowConsumerEmail") => Some(0x34),
        (Search, "Search") => Some(0x05),
        (Search, "Store") => Some(0x07),
        (Search, "Name") => Some(0x08),
        (Search, "Query") => Some(0x09),
        (Search, "Options") => Some(0x0A),
        (Search, "Range") => Some(0x0B),
        (Search, "Status") => Some(0x0C),
        (Search, "Response") => Some(0x0D),
        (Search, "Result") => Some(0x0E),
        (Search, "Properties") => Some(0x0F),
        (Search, "Total") => Some(0x10),
        (Search, "EqualTo") => Some(0x11),
        (Search, "Value") => Some(0x12),
        (Search, "And") => Some(0x13),
        (Search, "Or") => Some(0x14),
        (Search, "FreeText") => Some(0x15),
        (Search, "DeepTraversal") => Some(0x17),
        (Search, "LongId") => Some(0x18),
        (AirSyncBase, "BodyPreference") => Some(0x05),
        (AirSyncBase, "Type") => Some(0x06),
        (AirSyncBase, "TruncationSize") => Some(0x07),
        (AirSyncBase, "AllOrNone") => Some(0x08),
        (AirSyncBase, "Body") => Some(0x0A),
        (AirSyncBase, "Data") => Some(0x0B),
        (AirSyncBase, "EstimatedDataSize") => Some(0x0C),
        (AirSyncBase, "Truncated") => Some(0x0D),
        (AirSyncBase, "Attachments") => Some(0x0E),
        (AirSyncBase, "Attachment") => Some(0x0F),
        (AirSyncBase, "DisplayName") => Some(0x10),
        (AirSyncBase, "FileReference") => Some(0x11),
        (AirSyncBase, "Method") => Some(0x12),
        (AirSyncBase, "ContentId") => Some(0x13),
        (AirSyncBase, "ContentLocation") => Some(0x14),
        (AirSyncBase, "IsInline") => Some(0x15),
        (AirSyncBase, "ContentType") => Some(0x17),
        (AirSyncBase, "Preview") => Some(0x18),
        (Settings, "Status") => Some(0x06),
        (Settings, "Set") => Some(0x08),
        (Settings, "DeviceInformation") => Some(0x16),
        (Settings, "Model") => Some(0x17),
        (Settings, "IMEI") => Some(0x18),
        (Settings, "FriendlyName") => Some(0x19),
        (Settings, "OS") => Some(0x1A),
        (Settings, "OSLanguage") => Some(0x1B),
        (Settings, "PhoneNumber") => Some(0x1C),
        (Settings, "UserAgent") => Some(0x20),
        (Settings, "MobileOperator") => Some(0x22),
        (ItemOperations, "ItemOperations") => Some(0x05),
        (ItemOperations, "Fetch") => Some(0x06),
        (ItemOperations, "Store") => Some(0x07),
        (ItemOperations, "Options") => Some(0x08),
        (ItemOperations, "Range") => Some(0x09),
        (ItemOperations, "Total") => Some(0x0A),
        (ItemOperations, "Properties") => Some(0x0B),
        (ItemOperations, "Data") => Some(0x0C),
        (ItemOperations, "Status") => Some(0x0D),
        (ItemOperations, "Response") => Some(0x0E),
        (ItemOperations, "Part") => Some(0x11),
        (ComposeMail, "SendMail") => Some(0x05),
        (ComposeMail, "SmartForward") => Some(0x06),
        (ComposeMail, "SmartReply") => Some(0x07),
        (ComposeMail, "SaveInSentItems") => Some(0x08),
        (ComposeMail, "ReplaceMime") => Some(0x09),
        (ComposeMail, "Source") => Some(0x0B),
        (ComposeMail, "FolderId") => Some(0x0C),
        (ComposeMail, "ItemId") => Some(0x0D),
        (ComposeMail, "LongId") => Some(0x0E),
        (ComposeMail, "InstanceId") => Some(0x0F),
        (ComposeMail, "Mime") => Some(0x10),
        (ComposeMail, "ClientId") => Some(0x11),
        (ComposeMail, "Status") => Some(0x12),
        (ComposeMail, "AccountId") => Some(0x13),
        _ => None,
    }
}

fn name_for(page: WbxmlCodePage, token: u8) -> Option<&'static str> {
    use WbxmlCodePage::*;
    match (page, token) {
        (AirSync, 0x05) => Some("Sync"),
        (AirSync, 0x06) => Some("Responses"),
        (AirSync, 0x07) => Some("Add"),
        (AirSync, 0x08) => Some("Change"),
        (AirSync, 0x09) => Some("Delete"),
        (AirSync, 0x0A) => Some("Fetch"),
        (AirSync, 0x0B) => Some("SyncKey"),
        (AirSync, 0x0C) => Some("ClientId"),
        (AirSync, 0x0D) => Some("ServerId"),
        (AirSync, 0x0E) => Some("Status"),
        (AirSync, 0x0F) => Some("Collection"),
        (AirSync, 0x10) => Some("Class"),
        (AirSync, 0x12) => Some("CollectionId"),
        (AirSync, 0x13) => Some("GetChanges"),
        (AirSync, 0x14) => Some("MoreAvailable"),
        (AirSync, 0x15) => Some("WindowSize"),
        (AirSync, 0x16) => Some("Commands"),
        (AirSync, 0x17) => Some("Options"),
        (AirSync, 0x18) => Some("FilterType"),
        (AirSync, 0x1B) => Some("Conflict"),
        (AirSync, 0x1C) => Some("Collections"),
        (AirSync, 0x1D) => Some("ApplicationData"),
        (AirSync, 0x1E) => Some("DeletesAsMoves"),
        (AirSync, 0x20) => Some("Supported"),
        (AirSync, 0x21) => Some("SoftDelete"),
        (AirSync, 0x22) => Some("MIMESupport"),
        (AirSync, 0x23) => Some("MIMETruncation"),
        (AirSync, 0x24) => Some("Wait"),
        (AirSync, 0x25) => Some("Limit"),
        (AirSync, 0x26) => Some("Partial"),
        (AirSync, 0x27) => Some("ConversationMode"),
        (AirSync, 0x28) => Some("MaxItems"),
        (AirSync, 0x29) => Some("HeartbeatInterval"),
        (Contacts, 0x09) => Some("Body"),
        (Contacts, 0x13) => Some("BusinessPhoneNumber"),
        (Contacts, 0x19) => Some("CompanyName"),
        (Contacts, 0x1B) => Some("Email1Address"),
        (Contacts, 0x1E) => Some("FileAs"),
        (Contacts, 0x1F) => Some("FirstName"),
        (Contacts, 0x27) => Some("HomePhoneNumber"),
        (Contacts, 0x28) => Some("JobTitle"),
        (Contacts, 0x29) => Some("LastName"),
        (Contacts, 0x2B) => Some("MobilePhoneNumber"),
        (Calendar, 0x05) => Some("TimeZone"),
        (Calendar, 0x06) => Some("AllDayEvent"),
        (Email, 0x0F) => Some("DateReceived"),
        (Email, 0x11) => Some("DisplayTo"),
        (Email, 0x12) => Some("Importance"),
        (Email, 0x13) => Some("MessageClass"),
        (Email, 0x14) => Some("Subject"),
        (Email, 0x15) => Some("Read"),
        (Email, 0x16) => Some("To"),
        (Email, 0x17) => Some("Cc"),
        (Email, 0x18) => Some("From"),
        (Email, 0x19) => Some("ReplyTo"),
        (Email, 0x3A) => Some("Flag"),
        (Email, 0x3B) => Some("Status"),
        (Email, 0x3D) => Some("FlagType"),
        (Email, 0x3E) => Some("CompleteTime"),
        (Calendar, 0x07) => Some("Attendees"),
        (Calendar, 0x08) => Some("Attendee"),
        (Calendar, 0x09) => Some("Email"),
        (Calendar, 0x0A) => Some("Name"),
        (Calendar, 0x0D) => Some("BusyStatus"),
        (Calendar, 0x12) => Some("EndTime"),
        (Calendar, 0x17) => Some("Location"),
        (Calendar, 0x18) => Some("MeetingStatus"),
        (Calendar, 0x19) => Some("OrganizerEmail"),
        (Calendar, 0x1A) => Some("OrganizerName"),
        (Calendar, 0x1B) => Some("Recurrence"),
        (Calendar, 0x1C) => Some("Type"),
        (Calendar, 0x1D) => Some("Until"),
        (Calendar, 0x1E) => Some("Occurrences"),
        (Calendar, 0x1F) => Some("Interval"),
        (Calendar, 0x20) => Some("DayOfWeek"),
        (Calendar, 0x21) => Some("DayOfMonth"),
        (Calendar, 0x22) => Some("WeekOfMonth"),
        (Calendar, 0x23) => Some("MonthOfYear"),
        (Calendar, 0x24) => Some("Reminder"),
        (Calendar, 0x26) => Some("Subject"),
        (Calendar, 0x27) => Some("StartTime"),
        (Calendar, 0x28) => Some("UID"),
        (Calendar, 0x29) => Some("AttendeeStatus"),
        (Calendar, 0x2A) => Some("AttendeeType"),
        (Tasks, 0x0B) => Some("DateCompleted"),
        (Tasks, 0x0C) => Some("DueDate"),
        (Tasks, 0x0D) => Some("UtcDueDate"),
        (Tasks, 0x1E) => Some("StartDate"),
        (Tasks, 0x1F) => Some("UtcStartDate"),
        (Move, 0x05) => Some("MoveItems"),
        (Move, 0x06) => Some("Move"),
        (Move, 0x07) => Some("SrcMsgId"),
        (Move, 0x08) => Some("SrcFldId"),
        (Move, 0x09) => Some("DstFldId"),
        (Move, 0x0A) => Some("Response"),
        (Move, 0x0B) => Some("Status"),
        (Move, 0x0C) => Some("DstMsgId"),
        (GetItemEstimate, 0x05) => Some("GetItemEstimate"),
        (GetItemEstimate, 0x07) => Some("Collections"),
        (GetItemEstimate, 0x08) => Some("Collection"),
        (GetItemEstimate, 0x09) => Some("Class"),
        (GetItemEstimate, 0x0A) => Some("CollectionId"),
        (GetItemEstimate, 0x0C) => Some("Estimate"),
        (GetItemEstimate, 0x0D) => Some("Response"),
        (GetItemEstimate, 0x0E) => Some("Status"),
        (FolderHierarchy, 0x07) => Some("DisplayName"),
        (FolderHierarchy, 0x08) => Some("ServerId"),
        (FolderHierarchy, 0x09) => Some("ParentId"),
        (FolderHierarchy, 0x0A) => Some("Type"),
        (FolderHierarchy, 0x0C) => Some("Status"),
        (FolderHierarchy, 0x0E) => Some("Changes"),
        (FolderHierarchy, 0x0F) => Some("Add"),
        (FolderHierarchy, 0x10) => Some("Delete"),
        (FolderHierarchy, 0x11) => Some("Update"),
        (FolderHierarchy, 0x12) => Some("SyncKey"),
        (FolderHierarchy, 0x13) => Some("FolderCreate"),
        (FolderHierarchy, 0x14) => Some("FolderDelete"),
        (FolderHierarchy, 0x15) => Some("FolderUpdate"),
        (FolderHierarchy, 0x16) => Some("FolderSync"),
        (FolderHierarchy, 0x17) => Some("Count"),
        (Ping, 0x05) => Some("Ping"),
        (Ping, 0x07) => Some("Status"),
        (Ping, 0x08) => Some("HeartbeatInterval"),
        (Ping, 0x09) => Some("Folders"),
        (Ping, 0x0A) => Some("Folder"),
        (Ping, 0x0B) => Some("Id"),
        (Ping, 0x0C) => Some("Class"),
        (Ping, 0x0D) => Some("MaxFolders"),
        (Provision, 0x05) => Some("Provision"),
        (Provision, 0x06) => Some("Policies"),
        (Provision, 0x07) => Some("Policy"),
        (Provision, 0x08) => Some("PolicyType"),
        (Provision, 0x09) => Some("PolicyKey"),
        (Provision, 0x0A) => Some("Data"),
        (Provision, 0x0B) => Some("Status"),
        (Provision, 0x0D) => Some("EASProvisionDoc"),
        (Provision, 0x0E) => Some("DevicePasswordEnabled"),
        (Provision, 0x0F) => Some("AlphanumericDevicePasswordRequired"),
        (Provision, 0x13) => Some("AttachmentsEnabled"),
        (Provision, 0x14) => Some("MinDevicePasswordLength"),
        (Provision, 0x18) => Some("AllowSimpleDevicePassword"),
        (Provision, 0x1B) => Some("AllowStorageCard"),
        (Provision, 0x1C) => Some("AllowCamera"),
        (Provision, 0x1D) => Some("RequireDeviceEncryption"),
        (Provision, 0x21) => Some("AllowWiFi"),
        (Provision, 0x22) => Some("AllowTextMessaging"),
        (Provision, 0x23) => Some("AllowPOPIMAPEmail"),
        (Provision, 0x33) => Some("AllowBrowser"),
        (Provision, 0x34) => Some("AllowConsumerEmail"),
        (Search, 0x05) => Some("Search"),
        (Search, 0x07) => Some("Store"),
        (Search, 0x08) => Some("Name"),
        (Search, 0x09) => Some("Query"),
        (Search, 0x0A) => Some("Options"),
        (Search, 0x0B) => Some("Range"),
        (Search, 0x0C) => Some("Status"),
        (Search, 0x0D) => Some("Response"),
        (Search, 0x0E) => Some("Result"),
        (Search, 0x0F) => Some("Properties"),
        (Search, 0x10) => Some("Total"),
        (Search, 0x11) => Some("EqualTo"),
        (Search, 0x12) => Some("Value"),
        (Search, 0x13) => Some("And"),
        (Search, 0x14) => Some("Or"),
        (Search, 0x15) => Some("FreeText"),
        (Search, 0x17) => Some("DeepTraversal"),
        (Search, 0x18) => Some("LongId"),
        (AirSyncBase, 0x05) => Some("BodyPreference"),
        (AirSyncBase, 0x06) => Some("Type"),
        (AirSyncBase, 0x07) => Some("TruncationSize"),
        (AirSyncBase, 0x08) => Some("AllOrNone"),
        (AirSyncBase, 0x0A) => Some("Body"),
        (AirSyncBase, 0x0B) => Some("Data"),
        (AirSyncBase, 0x0C) => Some("EstimatedDataSize"),
        (AirSyncBase, 0x0D) => Some("Truncated"),
        (AirSyncBase, 0x0E) => Some("Attachments"),
        (AirSyncBase, 0x0F) => Some("Attachment"),
        (AirSyncBase, 0x10) => Some("DisplayName"),
        (AirSyncBase, 0x11) => Some("FileReference"),
        (AirSyncBase, 0x12) => Some("Method"),
        (AirSyncBase, 0x13) => Some("ContentId"),
        (AirSyncBase, 0x14) => Some("ContentLocation"),
        (AirSyncBase, 0x15) => Some("IsInline"),
        (AirSyncBase, 0x17) => Some("ContentType"),
        (AirSyncBase, 0x18) => Some("Preview"),
        (Settings, 0x06) => Some("Status"),
        (Settings, 0x08) => Some("Set"),
        (Settings, 0x16) => Some("DeviceInformation"),
        (Settings, 0x17) => Some("Model"),
        (Settings, 0x18) => Some("IMEI"),
        (Settings, 0x19) => Some("FriendlyName"),
        (Settings, 0x1A) => Some("OS"),
        (Settings, 0x1B) => Some("OSLanguage"),
        (Settings, 0x1C) => Some("PhoneNumber"),
        (Settings, 0x20) => Some("UserAgent"),
        (Settings, 0x22) => Some("MobileOperator"),
        (ItemOperations, 0x05) => Some("ItemOperations"),
        (ItemOperations, 0x06) => Some("Fetch"),
        (ItemOperations, 0x07) => Some("Store"),
        (ItemOperations, 0x08) => Some("Options"),
        (ItemOperations, 0x09) => Some("Range"),
        (ItemOperations, 0x0A) => Some("Total"),
        (ItemOperations, 0x0B) => Some("Properties"),
        (ItemOperations, 0x0C) => Some("Data"),
        (ItemOperations, 0x0D) => Some("Status"),
        (ItemOperations, 0x0E) => Some("Response"),
        (ItemOperations, 0x11) => Some("Part"),
        (ComposeMail, 0x05) => Some("SendMail"),
        (ComposeMail, 0x06) => Some("SmartForward"),
        (ComposeMail, 0x07) => Some("SmartReply"),
        (ComposeMail, 0x08) => Some("SaveInSentItems"),
        (ComposeMail, 0x09) => Some("ReplaceMime"),
        (ComposeMail, 0x0B) => Some("Source"),
        (ComposeMail, 0x0C) => Some("FolderId"),
        (ComposeMail, 0x0D) => Some("ItemId"),
        (ComposeMail, 0x0E) => Some("LongId"),
        (ComposeMail, 0x0F) => Some("InstanceId"),
        (ComposeMail, 0x10) => Some("Mime"),
        (ComposeMail, 0x11) => Some("ClientId"),
        (ComposeMail, 0x12) => Some("Status"),
        (ComposeMail, 0x13) => Some("AccountId"),
        _ => None,
    }
}
