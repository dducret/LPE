use anyhow::{bail, Result};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveSyncCommand {
    Sync,
    SendMail,
    SmartForward,
    SmartReply,
    GetAttachment,
    FolderSync,
    FolderCreate,
    FolderDelete,
    FolderUpdate,
    MoveItems,
    GetItemEstimate,
    MeetingResponse,
    Search,
    Settings,
    Ping,
    ItemOperations,
    Provision,
    ResolveRecipients,
    ValidateCert,
    Find,
}

impl ActiveSyncCommand {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "Sync",
            Self::SendMail => "SendMail",
            Self::SmartForward => "SmartForward",
            Self::SmartReply => "SmartReply",
            Self::GetAttachment => "GetAttachment",
            Self::FolderSync => "FolderSync",
            Self::FolderCreate => "FolderCreate",
            Self::FolderDelete => "FolderDelete",
            Self::FolderUpdate => "FolderUpdate",
            Self::MoveItems => "MoveItems",
            Self::GetItemEstimate => "GetItemEstimate",
            Self::MeetingResponse => "MeetingResponse",
            Self::Search => "Search",
            Self::Settings => "Settings",
            Self::Ping => "Ping",
            Self::ItemOperations => "ItemOperations",
            Self::Provision => "Provision",
            Self::ResolveRecipients => "ResolveRecipients",
            Self::ValidateCert => "ValidateCert",
            Self::Find => "Find",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self> {
        match value {
            "Sync" => Ok(Self::Sync),
            "SendMail" => Ok(Self::SendMail),
            "SmartForward" => Ok(Self::SmartForward),
            "SmartReply" => Ok(Self::SmartReply),
            "GetAttachment" => Ok(Self::GetAttachment),
            "FolderSync" => Ok(Self::FolderSync),
            "FolderCreate" => Ok(Self::FolderCreate),
            "FolderDelete" => Ok(Self::FolderDelete),
            "FolderUpdate" => Ok(Self::FolderUpdate),
            "MoveItems" => Ok(Self::MoveItems),
            "GetItemEstimate" => Ok(Self::GetItemEstimate),
            "MeetingResponse" => Ok(Self::MeetingResponse),
            "Search" => Ok(Self::Search),
            "Settings" => Ok(Self::Settings),
            "Ping" => Ok(Self::Ping),
            "ItemOperations" => Ok(Self::ItemOperations),
            "Provision" => Ok(Self::Provision),
            "ResolveRecipients" => Ok(Self::ResolveRecipients),
            "ValidateCert" => Ok(Self::ValidateCert),
            "Find" => Ok(Self::Find),
            _ => {
                tracing::warn!(
                    adapter = "activesync",
                    enum_name = "ActiveSyncCommand",
                    raw_value = value,
                    "unsupported ActiveSync command"
                );
                bail!("unsupported ActiveSync command: {value}")
            }
        }
    }

    pub(crate) fn from_code(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Sync),
            1 => Ok(Self::SendMail),
            2 => Ok(Self::SmartForward),
            3 => Ok(Self::SmartReply),
            4 => Ok(Self::GetAttachment),
            9 => Ok(Self::FolderSync),
            10 => Ok(Self::FolderCreate),
            11 => Ok(Self::FolderDelete),
            12 => Ok(Self::FolderUpdate),
            13 => Ok(Self::MoveItems),
            14 => Ok(Self::GetItemEstimate),
            15 => Ok(Self::MeetingResponse),
            16 => Ok(Self::Search),
            17 => Ok(Self::Settings),
            18 => Ok(Self::Ping),
            19 => Ok(Self::ItemOperations),
            20 => Ok(Self::Provision),
            21 => Ok(Self::ResolveRecipients),
            22 => Ok(Self::ValidateCert),
            23 => Ok(Self::Find),
            _ => {
                tracing::warn!(
                    adapter = "activesync",
                    enum_name = "ActiveSyncCommand",
                    raw_value = value,
                    "unsupported ActiveSync command code"
                );
                bail!("unsupported ActiveSync command code")
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn known_unsupported_name(value: u8) -> Option<&'static str> {
        match value {
            4 => Some("GetAttachment"),
            15 => Some("MeetingResponse"),
            17 => Some("Settings"),
            21 => Some("ResolveRecipients"),
            22 => Some("ValidateCert"),
            _ => None,
        }
    }
}

impl fmt::Display for ActiveSyncCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ActiveSyncStatus {
    Success,
    ProtocolError,
    ServerError,
    InvalidSyncKey,
    InvalidRequest,
    NotFound,
    InvalidDestination,
    SameSourceAndDestination,
    FolderExists,
    FolderSyncRequired,
    FolderParentNotFound,
    FolderSystemRejected,
    HierarchyChanged,
    PolicyRequired,
    SendMailInvalidMime,
    SendMailServerError,
    SendMailSourceNotFound,
    SendMailMailboxAccessDenied,
    ItemOperationsNotFound,
    ItemOperationsInvalidStore,
    PingChanges,
    PingMissingParameters,
    PingIntervalOutOfRange,
    PingTooManyFolders,
    PingFolderSyncRequired,
}

impl ActiveSyncStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Success => "1",
            Self::ProtocolError => "2",
            Self::ServerError => "6",
            Self::InvalidSyncKey => "3",
            Self::InvalidRequest => "4",
            Self::NotFound => "8",
            Self::InvalidDestination => "2",
            Self::SameSourceAndDestination => "4",
            Self::FolderExists => "2",
            Self::FolderSyncRequired => "9",
            Self::FolderParentNotFound => "5",
            Self::FolderSystemRejected => "10",
            Self::HierarchyChanged => "12",
            Self::PolicyRequired => "142",
            Self::SendMailInvalidMime => "107",
            Self::SendMailServerError => "120",
            Self::SendMailSourceNotFound => "150",
            Self::SendMailMailboxAccessDenied => "166",
            Self::ItemOperationsNotFound => "15",
            Self::ItemOperationsInvalidStore => "6",
            Self::PingChanges => "2",
            Self::PingMissingParameters => "3",
            Self::PingIntervalOutOfRange => "5",
            Self::PingTooManyFolders => "6",
            Self::PingFolderSyncRequired => "7",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActiveSyncFolderType {
    Inbox,
    Drafts,
    DeletedItems,
    SentItems,
    Calendar,
    Contacts,
    UserCreatedMail,
}

impl ActiveSyncFolderType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "2",
            Self::Drafts => "3",
            Self::DeletedItems => "4",
            Self::SentItems => "5",
            Self::Calendar => "8",
            Self::Contacts => "9",
            Self::UserCreatedMail => "12",
        }
    }

    pub(crate) fn from_mailbox_role(role: &str) -> Self {
        match role {
            "inbox" => Self::Inbox,
            "drafts" => Self::Drafts,
            "trash" => Self::DeletedItems,
            "sent" => Self::SentItems,
            _ => Self::UserCreatedMail,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyPreferenceType {
    PlainText,
    Html,
    Mime,
}

impl BodyPreferenceType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::PlainText => "1",
            Self::Html => "2",
            Self::Mime => "4",
        }
    }

    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::PlainText),
            2 => Some(Self::Html),
            4 => Some(Self::Mime),
            _ => {
                tracing::warn!(
                    adapter = "activesync",
                    enum_name = "BodyPreferenceType",
                    raw_value = value,
                    "unsupported ActiveSync body preference type"
                );
                None
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WbxmlCodePage {
    AirSync = 0,
    Contacts = 1,
    Email = 2,
    Calendar = 4,
    Tasks = 9,
    Move = 5,
    GetItemEstimate = 6,
    FolderHierarchy = 7,
    Ping = 13,
    Provision = 14,
    Search = 15,
    AirSyncBase = 17,
    Settings = 18,
    ItemOperations = 20,
    ComposeMail = 21,
}

impl WbxmlCodePage {
    pub(crate) fn as_u8(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for WbxmlCodePage {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::AirSync),
            1 => Ok(Self::Contacts),
            2 => Ok(Self::Email),
            4 => Ok(Self::Calendar),
            9 => Ok(Self::Tasks),
            5 => Ok(Self::Move),
            6 => Ok(Self::GetItemEstimate),
            7 => Ok(Self::FolderHierarchy),
            13 => Ok(Self::Ping),
            14 => Ok(Self::Provision),
            15 => Ok(Self::Search),
            17 => Ok(Self::AirSyncBase),
            18 => Ok(Self::Settings),
            20 => Ok(Self::ItemOperations),
            21 => Ok(Self::ComposeMail),
            _ => {
                tracing::warn!(
                    adapter = "activesync",
                    enum_name = "WbxmlCodePage",
                    raw_value = value,
                    "unsupported WBXML code page"
                );
                bail!("unsupported WBXML code page")
            }
        }
    }
}

impl WbxmlCodePage {
    #[allow(dead_code)]
    pub(crate) fn known_unsupported_name(value: u8) -> Option<&'static str> {
        match value {
            3 => Some("AirNotify"),
            8 => Some("MeetingResponse"),
            10 => Some("ResolveRecipients"),
            11 => Some("ValidateCert"),
            12 => Some("Contacts2"),
            16 => Some("GAL"),
            19 => Some("DocumentLibrary"),
            22 => Some("Email2"),
            23 => Some("Notes"),
            24 => Some("RightsManagement"),
            25 => Some("Find"),
            _ => None,
        }
    }
}

impl From<WbxmlCodePage> for u8 {
    fn from(value: WbxmlCodePage) -> Self {
        value.as_u8()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activesync_command_codes_match_ms_ashttp() {
        let commands = [
            (0, ActiveSyncCommand::Sync),
            (1, ActiveSyncCommand::SendMail),
            (2, ActiveSyncCommand::SmartForward),
            (3, ActiveSyncCommand::SmartReply),
            (4, ActiveSyncCommand::GetAttachment),
            (9, ActiveSyncCommand::FolderSync),
            (10, ActiveSyncCommand::FolderCreate),
            (11, ActiveSyncCommand::FolderDelete),
            (12, ActiveSyncCommand::FolderUpdate),
            (13, ActiveSyncCommand::MoveItems),
            (14, ActiveSyncCommand::GetItemEstimate),
            (15, ActiveSyncCommand::MeetingResponse),
            (16, ActiveSyncCommand::Search),
            (17, ActiveSyncCommand::Settings),
            (18, ActiveSyncCommand::Ping),
            (19, ActiveSyncCommand::ItemOperations),
            (20, ActiveSyncCommand::Provision),
            (21, ActiveSyncCommand::ResolveRecipients),
            (22, ActiveSyncCommand::ValidateCert),
            (23, ActiveSyncCommand::Find),
        ];

        for (code, command) in commands {
            assert_eq!(ActiveSyncCommand::from_code(code).unwrap(), command);
        }
        assert!(ActiveSyncCommand::from_code(5).is_err());
        assert_eq!(
            ActiveSyncCommand::known_unsupported_name(15),
            Some("MeetingResponse")
        );
    }

    #[test]
    fn wbxml_code_pages_match_bounded_ms_aswbxml_manifest() {
        let supported = [
            (0, WbxmlCodePage::AirSync),
            (1, WbxmlCodePage::Contacts),
            (2, WbxmlCodePage::Email),
            (4, WbxmlCodePage::Calendar),
            (5, WbxmlCodePage::Move),
            (6, WbxmlCodePage::GetItemEstimate),
            (7, WbxmlCodePage::FolderHierarchy),
            (9, WbxmlCodePage::Tasks),
            (13, WbxmlCodePage::Ping),
            (14, WbxmlCodePage::Provision),
            (15, WbxmlCodePage::Search),
            (17, WbxmlCodePage::AirSyncBase),
            (18, WbxmlCodePage::Settings),
            (20, WbxmlCodePage::ItemOperations),
            (21, WbxmlCodePage::ComposeMail),
        ];

        for (code_page, expected) in supported {
            assert_eq!(WbxmlCodePage::try_from(code_page).unwrap(), expected);
        }
        assert!(WbxmlCodePage::try_from(25).is_err());
        assert_eq!(WbxmlCodePage::known_unsupported_name(25), Some("Find"));
    }

    #[test]
    fn active_sync_status_folder_and_body_values_are_manifest_checked() {
        let statuses = [
            (ActiveSyncStatus::Success, "1"),
            (ActiveSyncStatus::ProtocolError, "2"),
            (ActiveSyncStatus::ServerError, "6"),
            (ActiveSyncStatus::InvalidSyncKey, "3"),
            (ActiveSyncStatus::InvalidRequest, "4"),
            (ActiveSyncStatus::NotFound, "8"),
            (ActiveSyncStatus::InvalidDestination, "2"),
            (ActiveSyncStatus::SameSourceAndDestination, "4"),
            (ActiveSyncStatus::FolderExists, "2"),
            (ActiveSyncStatus::FolderSyncRequired, "9"),
            (ActiveSyncStatus::FolderParentNotFound, "5"),
            (ActiveSyncStatus::FolderSystemRejected, "10"),
            (ActiveSyncStatus::HierarchyChanged, "12"),
            (ActiveSyncStatus::PolicyRequired, "142"),
            (ActiveSyncStatus::SendMailInvalidMime, "107"),
            (ActiveSyncStatus::SendMailServerError, "120"),
            (ActiveSyncStatus::SendMailSourceNotFound, "150"),
            (ActiveSyncStatus::SendMailMailboxAccessDenied, "166"),
            (ActiveSyncStatus::ItemOperationsNotFound, "15"),
            (ActiveSyncStatus::ItemOperationsInvalidStore, "6"),
            (ActiveSyncStatus::PingChanges, "2"),
            (ActiveSyncStatus::PingMissingParameters, "3"),
            (ActiveSyncStatus::PingIntervalOutOfRange, "5"),
            (ActiveSyncStatus::PingTooManyFolders, "6"),
            (ActiveSyncStatus::PingFolderSyncRequired, "7"),
        ];
        for (status, value) in statuses {
            assert_eq!(status.as_str(), value);
        }

        let folder_types = [
            (ActiveSyncFolderType::Inbox, "2"),
            (ActiveSyncFolderType::Drafts, "3"),
            (ActiveSyncFolderType::DeletedItems, "4"),
            (ActiveSyncFolderType::SentItems, "5"),
            (ActiveSyncFolderType::Calendar, "8"),
            (ActiveSyncFolderType::Contacts, "9"),
            (ActiveSyncFolderType::UserCreatedMail, "12"),
        ];
        for (folder_type, value) in folder_types {
            assert_eq!(folder_type.as_str(), value);
        }

        let body_preferences = [
            (1, BodyPreferenceType::PlainText),
            (2, BodyPreferenceType::Html),
            (4, BodyPreferenceType::Mime),
        ];
        for (value, expected) in body_preferences {
            assert_eq!(BodyPreferenceType::from_u8(value), Some(expected));
            assert_eq!(expected.as_str(), value.to_string());
        }
        assert_eq!(BodyPreferenceType::from_u8(3), None);
    }
}
