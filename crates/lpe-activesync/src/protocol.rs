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

impl From<WbxmlCodePage> for u8 {
    fn from(value: WbxmlCodePage) -> Self {
        value.as_u8()
    }
}
