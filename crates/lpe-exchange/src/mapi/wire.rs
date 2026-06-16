#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum MapiHttpRequestType {
    Connect,
    Disconnect,
    Execute,
    NotificationWait,
    Bind,
    Unbind,
    CompareMids,
    DnToEph,
    DnToMid,
    GetMatches,
    GetPropList,
    GetProps,
    GetSpecialTable,
    GetTemplateInfo,
    ModLinkAtt,
    ModProps,
    GetAddressBookUrl,
    GetMailboxUrl,
    QueryColumns,
    QueryRows,
    ResolveNames,
    ResortRestriction,
    SeekEntries,
    UpdateStat,
    Ping,
    Unsupported(String),
}

impl MapiHttpRequestType {
    #[allow(dead_code)]
    pub(in crate::mapi) const DOCUMENTED_SUPPORTED_VALUES: &'static [(&'static str, Self)] = &[
        ("Connect", Self::Connect),
        ("Disconnect", Self::Disconnect),
        ("Execute", Self::Execute),
        ("NotificationWait", Self::NotificationWait),
        ("Bind", Self::Bind),
        ("Unbind", Self::Unbind),
        ("CompareMIds", Self::CompareMids),
        ("DNToEPH", Self::DnToEph),
        ("DNToMId", Self::DnToMid),
        ("GetMatches", Self::GetMatches),
        ("GetPropList", Self::GetPropList),
        ("GetProps", Self::GetProps),
        ("GetSpecialTable", Self::GetSpecialTable),
        ("GetTemplateInfo", Self::GetTemplateInfo),
        ("ModLinkAtt", Self::ModLinkAtt),
        ("ModProps", Self::ModProps),
        ("GetAddressBookUrl", Self::GetAddressBookUrl),
        ("GetMailboxUrl", Self::GetMailboxUrl),
        ("QueryColumns", Self::QueryColumns),
        ("QueryRows", Self::QueryRows),
        ("ResolveNames", Self::ResolveNames),
        ("ResortRestriction", Self::ResortRestriction),
        ("SeekEntries", Self::SeekEntries),
        ("UpdateStat", Self::UpdateStat),
        ("PING", Self::Ping),
    ];

    pub(in crate::mapi) fn header_value(&self) -> &str {
        match self {
            Self::Connect => "Connect",
            Self::Disconnect => "Disconnect",
            Self::Execute => "Execute",
            Self::NotificationWait => "NotificationWait",
            Self::Bind => "Bind",
            Self::Unbind => "Unbind",
            Self::CompareMids => "CompareMIds",
            Self::DnToEph => "DNToEPH",
            Self::DnToMid => "DNToMId",
            Self::GetMatches => "GetMatches",
            Self::GetPropList => "GetPropList",
            Self::GetProps => "GetProps",
            Self::GetSpecialTable => "GetSpecialTable",
            Self::GetTemplateInfo => "GetTemplateInfo",
            Self::ModLinkAtt => "ModLinkAtt",
            Self::ModProps => "ModProps",
            Self::GetAddressBookUrl => "GetAddressBookUrl",
            Self::GetMailboxUrl => "GetMailboxUrl",
            Self::QueryColumns => "QueryColumns",
            Self::QueryRows => "QueryRows",
            Self::ResolveNames => "ResolveNames",
            Self::ResortRestriction => "ResortRestriction",
            Self::SeekEntries => "SeekEntries",
            Self::UpdateStat => "UpdateStat",
            Self::Ping => "PING",
            Self::Unsupported(value) => value,
        }
    }

    pub(in crate::mapi) fn requires_nspi_session(&self) -> bool {
        matches!(
            self,
            Self::CompareMids
                | Self::DnToEph
                | Self::GetMatches
                | Self::GetPropList
                | Self::GetProps
                | Self::GetSpecialTable
                | Self::GetTemplateInfo
                | Self::ModLinkAtt
                | Self::ModProps
                | Self::GetAddressBookUrl
                | Self::GetMailboxUrl
                | Self::QueryColumns
                | Self::QueryRows
                | Self::ResolveNames
                | Self::ResortRestriction
                | Self::SeekEntries
                | Self::UpdateStat
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)]
pub(in crate::mapi) enum RopId {
    Release = 0x01,
    OpenFolder = 0x02,
    OpenMessage = 0x03,
    GetHierarchyTable = 0x04,
    GetContentsTable = 0x05,
    CreateMessage = 0x06,
    GetPropertiesSpecific = 0x07,
    GetPropertiesAll = 0x08,
    GetPropertiesList = 0x09,
    SetProperties = 0x0A,
    DeleteProperties = 0x0B,
    SaveChangesMessage = 0x0C,
    RemoveAllRecipients = 0x0D,
    ModifyRecipients = 0x0E,
    ReadRecipients = 0x0F,
    ReloadCachedInformation = 0x10,
    SetMessageReadFlag = 0x11,
    SetColumns = 0x12,
    SortTable = 0x13,
    Restrict = 0x14,
    QueryRows = 0x15,
    GetStatus = 0x16,
    QueryPosition = 0x17,
    SeekRow = 0x18,
    SeekRowBookmark = 0x19,
    SeekRowFractional = 0x1A,
    CreateBookmark = 0x1B,
    CreateFolder = 0x1C,
    DeleteFolder = 0x1D,
    DeleteMessages = 0x1E,
    GetMessageStatus = 0x1F,
    SetMessageStatus = 0x20,
    GetAttachmentTable = 0x21,
    OpenAttachment = 0x22,
    CreateAttachment = 0x23,
    DeleteAttachment = 0x24,
    SaveChangesAttachment = 0x25,
    SetReceiveFolder = 0x26,
    GetReceiveFolder = 0x27,
    Reserved = 0x28,
    RegisterNotification = 0x29,
    Notify = 0x2A,
    OpenStream = 0x2B,
    ReadStream = 0x2C,
    WriteStream = 0x2D,
    SeekStream = 0x2E,
    SetStreamSize = 0x2F,
    SetSearchCriteria = 0x30,
    GetSearchCriteria = 0x31,
    SubmitMessage = 0x32,
    MoveCopyMessages = 0x33,
    AbortSubmit = 0x34,
    MoveFolder = 0x35,
    CopyFolder = 0x36,
    QueryColumnsAll = 0x37,
    Abort = 0x38,
    CopyTo = 0x39,
    GetPermissionsTable = 0x3E,
    GetRulesTable = 0x3F,
    ModifyPermissions = 0x40,
    ModifyRules = 0x41,
    GetOwningServers = 0x42,
    GetAddressTypes = 0x49,
    TransportSend = 0x4A,
    FastTransferSourceCopyMessages = 0x4B,
    FastTransferSourceCopyFolder = 0x4C,
    FastTransferSourceCopyTo = 0x4D,
    FastTransferSourceGetBuffer = 0x4E,
    FindRow = 0x4F,
    Progress = 0x50,
    TransportNewMail = 0x51,
    GetValidAttachments = 0x52,
    FastTransferDestinationConfigure = 0x53,
    FastTransferDestinationPutBuffer = 0x54,
    GetNamesFromPropertyIds = 0x55,
    GetPropertyIdsFromNames = 0x56,
    UpdateDeferredActionMessages = 0x57,
    EmptyFolder = 0x58,
    ExpandRow = 0x59,
    CollapseRow = 0x5A,
    LockRegionStream = 0x5B,
    UnlockRegionStream = 0x5C,
    CommitStream = 0x5D,
    GetStreamSize = 0x5E,
    QueryNamedProperties = 0x5F,
    GetPerUserLongTermIds = 0x60,
    GetPerUserGuid = 0x61,
    ReadPerUserInformation = 0x63,
    WritePerUserInformation = 0x64,
    SetReadFlags = 0x66,
    CopyProperties = 0x67,
    GetReceiveFolderTable = 0x68,
    FastTransferSourceCopyProperties = 0x69,
    GetCollapseState = 0x6B,
    SetCollapseState = 0x6C,
    GetTransportFolder = 0x6D,
    Pending = 0x6E,
    OptionsData = 0x6F,
    SynchronizationConfigure = 0x70,
    SynchronizationImportMessageChange = 0x72,
    SynchronizationImportHierarchyChange = 0x73,
    SynchronizationImportDeletes = 0x74,
    SynchronizationUploadStateStreamBegin = 0x75,
    SynchronizationUploadStateStreamContinue = 0x76,
    SynchronizationUploadStateStreamEnd = 0x77,
    SynchronizationImportMessageMove = 0x78,
    SetPropertiesNoReplicate = 0x79,
    DeletePropertiesNoReplicate = 0x7A,
    GetStoreState = 0x7B,
    SynchronizationOpenCollector = 0x7E,
    GetLocalReplicaIds = 0x7F,
    SynchronizationImportReadStateChanges = 0x80,
    ResetTable = 0x81,
    LongTermIdFromId = 0x43,
    IdFromLongTermId = 0x44,
    PublicFolderIsGhosted = 0x45,
    OpenEmbeddedMessage = 0x46,
    SetSpooler = 0x47,
    SpoolerLockMessage = 0x48,
    SynchronizationGetTransferState = 0x82,
    ReadPerUserInformationByLongTermId = 0x83,
    WritePerUserInformationByLongTermId = 0x84,
    QueryRowsExtended = 0x85,
    TellVersion = 0x86,
    SetMessageFlags = 0x87,
    CopyToStream = 0x3A,
    CloneStream = 0x3B,
    FreeBookmark = 0x89,
    WriteAndCommitStream = 0x90,
    HardDeleteMessages = 0x91,
    HardDeleteMessagesAndSubfolders = 0x92,
    SetLocalReplicaMidsetDeleted = 0x93,
    FastTransferDestinationPutBufferExtended = 0x9D,
    WriteStreamExtended = 0xA3,
    Logon = 0xFE,
}

impl RopId {
    #[allow(dead_code)]
    pub(in crate::mapi) const fn as_u8(self) -> u8 {
        self as u8
    }

    #[allow(dead_code)]
    pub(in crate::mapi) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Release),
            0x02 => Some(Self::OpenFolder),
            0x03 => Some(Self::OpenMessage),
            0x04 => Some(Self::GetHierarchyTable),
            0x05 => Some(Self::GetContentsTable),
            0x06 => Some(Self::CreateMessage),
            0x07 => Some(Self::GetPropertiesSpecific),
            0x08 => Some(Self::GetPropertiesAll),
            0x09 => Some(Self::GetPropertiesList),
            0x0A => Some(Self::SetProperties),
            0x0B => Some(Self::DeleteProperties),
            0x0C => Some(Self::SaveChangesMessage),
            0x0D => Some(Self::RemoveAllRecipients),
            0x0E => Some(Self::ModifyRecipients),
            0x0F => Some(Self::ReadRecipients),
            0x10 => Some(Self::ReloadCachedInformation),
            0x11 => Some(Self::SetMessageReadFlag),
            0x12 => Some(Self::SetColumns),
            0x13 => Some(Self::SortTable),
            0x14 => Some(Self::Restrict),
            0x15 => Some(Self::QueryRows),
            0x16 => Some(Self::GetStatus),
            0x17 => Some(Self::QueryPosition),
            0x18 => Some(Self::SeekRow),
            0x19 => Some(Self::SeekRowBookmark),
            0x1A => Some(Self::SeekRowFractional),
            0x1B => Some(Self::CreateBookmark),
            0x1C => Some(Self::CreateFolder),
            0x1D => Some(Self::DeleteFolder),
            0x1E => Some(Self::DeleteMessages),
            0x1F => Some(Self::GetMessageStatus),
            0x20 => Some(Self::SetMessageStatus),
            0x21 => Some(Self::GetAttachmentTable),
            0x22 => Some(Self::OpenAttachment),
            0x23 => Some(Self::CreateAttachment),
            0x24 => Some(Self::DeleteAttachment),
            0x25 => Some(Self::SaveChangesAttachment),
            0x26 => Some(Self::SetReceiveFolder),
            0x27 => Some(Self::GetReceiveFolder),
            0x28 => Some(Self::Reserved),
            0x29 => Some(Self::RegisterNotification),
            0x2A => Some(Self::Notify),
            0x2B => Some(Self::OpenStream),
            0x2C => Some(Self::ReadStream),
            0x2D => Some(Self::WriteStream),
            0x2E => Some(Self::SeekStream),
            0x2F => Some(Self::SetStreamSize),
            0x30 => Some(Self::SetSearchCriteria),
            0x31 => Some(Self::GetSearchCriteria),
            0x32 => Some(Self::SubmitMessage),
            0x33 => Some(Self::MoveCopyMessages),
            0x34 => Some(Self::AbortSubmit),
            0x35 => Some(Self::MoveFolder),
            0x36 => Some(Self::CopyFolder),
            0x37 => Some(Self::QueryColumnsAll),
            0x38 => Some(Self::Abort),
            0x39 => Some(Self::CopyTo),
            0x3E => Some(Self::GetPermissionsTable),
            0x3F => Some(Self::GetRulesTable),
            0x40 => Some(Self::ModifyPermissions),
            0x41 => Some(Self::ModifyRules),
            0x42 => Some(Self::GetOwningServers),
            0x3A => Some(Self::CopyToStream),
            0x3B => Some(Self::CloneStream),
            0x43 => Some(Self::LongTermIdFromId),
            0x44 => Some(Self::IdFromLongTermId),
            0x45 => Some(Self::PublicFolderIsGhosted),
            0x46 => Some(Self::OpenEmbeddedMessage),
            0x47 => Some(Self::SetSpooler),
            0x48 => Some(Self::SpoolerLockMessage),
            0x49 => Some(Self::GetAddressTypes),
            0x4A => Some(Self::TransportSend),
            0x4B => Some(Self::FastTransferSourceCopyMessages),
            0x4C => Some(Self::FastTransferSourceCopyFolder),
            0x4D => Some(Self::FastTransferSourceCopyTo),
            0x4E => Some(Self::FastTransferSourceGetBuffer),
            0x4F => Some(Self::FindRow),
            0x50 => Some(Self::Progress),
            0x51 => Some(Self::TransportNewMail),
            0x52 => Some(Self::GetValidAttachments),
            0x53 => Some(Self::FastTransferDestinationConfigure),
            0x54 => Some(Self::FastTransferDestinationPutBuffer),
            0x55 => Some(Self::GetNamesFromPropertyIds),
            0x56 => Some(Self::GetPropertyIdsFromNames),
            0x57 => Some(Self::UpdateDeferredActionMessages),
            0x58 => Some(Self::EmptyFolder),
            0x59 => Some(Self::ExpandRow),
            0x5A => Some(Self::CollapseRow),
            0x5B => Some(Self::LockRegionStream),
            0x5C => Some(Self::UnlockRegionStream),
            0x5D => Some(Self::CommitStream),
            0x5E => Some(Self::GetStreamSize),
            0x5F => Some(Self::QueryNamedProperties),
            0x60 => Some(Self::GetPerUserLongTermIds),
            0x61 => Some(Self::GetPerUserGuid),
            0x63 => Some(Self::ReadPerUserInformation),
            0x64 => Some(Self::WritePerUserInformation),
            0x66 => Some(Self::SetReadFlags),
            0x67 => Some(Self::CopyProperties),
            0x68 => Some(Self::GetReceiveFolderTable),
            0x69 => Some(Self::FastTransferSourceCopyProperties),
            0x6B => Some(Self::GetCollapseState),
            0x6C => Some(Self::SetCollapseState),
            0x6D => Some(Self::GetTransportFolder),
            0x6E => Some(Self::Pending),
            0x6F => Some(Self::OptionsData),
            0x70 => Some(Self::SynchronizationConfigure),
            0x72 => Some(Self::SynchronizationImportMessageChange),
            0x73 => Some(Self::SynchronizationImportHierarchyChange),
            0x74 => Some(Self::SynchronizationImportDeletes),
            0x75 => Some(Self::SynchronizationUploadStateStreamBegin),
            0x76 => Some(Self::SynchronizationUploadStateStreamContinue),
            0x77 => Some(Self::SynchronizationUploadStateStreamEnd),
            0x78 => Some(Self::SynchronizationImportMessageMove),
            0x79 => Some(Self::SetPropertiesNoReplicate),
            0x7A => Some(Self::DeletePropertiesNoReplicate),
            0x7B => Some(Self::GetStoreState),
            0x7E => Some(Self::SynchronizationOpenCollector),
            0x7F => Some(Self::GetLocalReplicaIds),
            0x80 => Some(Self::SynchronizationImportReadStateChanges),
            0x81 => Some(Self::ResetTable),
            0x82 => Some(Self::SynchronizationGetTransferState),
            0x83 => Some(Self::ReadPerUserInformationByLongTermId),
            0x84 => Some(Self::WritePerUserInformationByLongTermId),
            0x85 => Some(Self::QueryRowsExtended),
            0x86 => Some(Self::TellVersion),
            0x87 => Some(Self::SetMessageFlags),
            0x89 => Some(Self::FreeBookmark),
            0x90 => Some(Self::WriteAndCommitStream),
            0x91 => Some(Self::HardDeleteMessages),
            0x92 => Some(Self::HardDeleteMessagesAndSubfolders),
            0x93 => Some(Self::SetLocalReplicaMidsetDeleted),
            0x9D => Some(Self::FastTransferDestinationPutBufferExtended),
            0xA3 => Some(Self::WriteStreamExtended),
            0xFE => Some(Self::Logon),
            _ => {
                let known_unsupported_name = Self::known_unsupported_name(value);
                tracing::warn!(
                    adapter = "mapi",
                    enum_name = "RopId",
                    raw_value = value,
                    known_unsupported = known_unsupported_name.is_some(),
                    known_unsupported_name = known_unsupported_name.unwrap_or(""),
                    "unsupported MAPI ROP id"
                );
                None
            }
        }
    }

    #[allow(dead_code)]
    pub(in crate::mapi) fn is_reserved(value: u8) -> bool {
        matches!(value, 0x00 | 0x28 | 0x3C | 0x3D | 0x62 | 0x65 | 0x6A | 0x71)
    }

    pub(in crate::mapi) fn is_supported_by_dispatch(self) -> bool {
        matches!(
            self,
            Self::Release
                | Self::OpenFolder
                | Self::OpenMessage
                | Self::GetHierarchyTable
                | Self::GetContentsTable
                | Self::CreateMessage
                | Self::GetPropertiesSpecific
                | Self::GetPropertiesAll
                | Self::GetPropertiesList
                | Self::SetProperties
                | Self::DeleteProperties
                | Self::SaveChangesMessage
                | Self::RemoveAllRecipients
                | Self::ModifyRecipients
                | Self::ReadRecipients
                | Self::ReloadCachedInformation
                | Self::SetMessageReadFlag
                | Self::SetColumns
                | Self::SortTable
                | Self::Restrict
                | Self::QueryRows
                | Self::Abort
                | Self::GetStatus
                | Self::QueryPosition
                | Self::SeekRow
                | Self::SeekRowBookmark
                | Self::SeekRowFractional
                | Self::CreateFolder
                | Self::DeleteFolder
                | Self::DeleteMessages
                | Self::GetMessageStatus
                | Self::SetMessageStatus
                | Self::GetAttachmentTable
                | Self::OpenAttachment
                | Self::CreateAttachment
                | Self::DeleteAttachment
                | Self::SaveChangesAttachment
                | Self::SetReceiveFolder
                | Self::GetReceiveFolder
                | Self::RegisterNotification
                | Self::OpenStream
                | Self::ReadStream
                | Self::WriteStream
                | Self::SeekStream
                | Self::SetStreamSize
                | Self::SetSearchCriteria
                | Self::GetSearchCriteria
                | Self::SubmitMessage
                | Self::MoveCopyMessages
                | Self::AbortSubmit
                | Self::MoveFolder
                | Self::CopyFolder
                | Self::QueryColumnsAll
                | Self::CreateBookmark
                | Self::CopyToStream
                | Self::CloneStream
                | Self::GetPermissionsTable
                | Self::GetRulesTable
                | Self::ModifyPermissions
                | Self::ModifyRules
                | Self::GetOwningServers
                | Self::LongTermIdFromId
                | Self::IdFromLongTermId
                | Self::PublicFolderIsGhosted
                | Self::ResetTable
                | Self::OpenEmbeddedMessage
                | Self::SetSpooler
                | Self::SpoolerLockMessage
                | Self::GetAddressTypes
                | Self::TransportSend
                | Self::FastTransferSourceCopyMessages
                | Self::FastTransferSourceCopyFolder
                | Self::FastTransferSourceCopyTo
                | Self::FastTransferSourceGetBuffer
                | Self::FindRow
                | Self::Progress
                | Self::TransportNewMail
                | Self::GetValidAttachments
                | Self::FastTransferDestinationConfigure
                | Self::FastTransferDestinationPutBuffer
                | Self::GetNamesFromPropertyIds
                | Self::GetPropertyIdsFromNames
                | Self::UpdateDeferredActionMessages
                | Self::EmptyFolder
                | Self::HardDeleteMessages
                | Self::HardDeleteMessagesAndSubfolders
                | Self::ExpandRow
                | Self::CollapseRow
                | Self::LockRegionStream
                | Self::UnlockRegionStream
                | Self::CommitStream
                | Self::GetStreamSize
                | Self::QueryNamedProperties
                | Self::GetPerUserLongTermIds
                | Self::GetPerUserGuid
                | Self::ReadPerUserInformation
                | Self::WritePerUserInformation
                | Self::SetReadFlags
                | Self::GetReceiveFolderTable
                | Self::FastTransferSourceCopyProperties
                | Self::GetCollapseState
                | Self::SetCollapseState
                | Self::GetTransportFolder
                | Self::OptionsData
                | Self::SynchronizationConfigure
                | Self::SynchronizationImportMessageChange
                | Self::SynchronizationImportHierarchyChange
                | Self::SynchronizationImportDeletes
                | Self::SynchronizationUploadStateStreamBegin
                | Self::SynchronizationUploadStateStreamContinue
                | Self::SynchronizationUploadStateStreamEnd
                | Self::SynchronizationImportMessageMove
                | Self::SetPropertiesNoReplicate
                | Self::DeletePropertiesNoReplicate
                | Self::GetStoreState
                | Self::SynchronizationOpenCollector
                | Self::GetLocalReplicaIds
                | Self::SynchronizationImportReadStateChanges
                | Self::SynchronizationGetTransferState
                | Self::TellVersion
                | Self::FreeBookmark
                | Self::WriteAndCommitStream
                | Self::SetLocalReplicaMidsetDeleted
                | Self::FastTransferDestinationPutBufferExtended
                | Self::WriteStreamExtended
                | Self::Logon
        )
    }

    #[allow(dead_code)]
    pub(in crate::mapi) fn known_unsupported_name(value: u8) -> Option<&'static str> {
        match value {
            0x39 => Some("RopCopyTo"),
            0x46 => Some("RopOpenEmbeddedMessage"),
            0x47 => Some("RopSetSpooler"),
            0x48 => Some("RopSpoolerLockMessage"),
            0x51 => Some("RopTransportNewMail"),
            0x59 => Some("RopExpandRow"),
            0x5A => Some("RopCollapseRow"),
            0x5B => Some("RopLockRegionStream"),
            0x5C => Some("RopUnlockRegionStream"),
            0x86 => Some("RopTellVersion"),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub(in crate::mapi) enum MapiPropertyType {
    Integer16 = 0x0002,
    Integer32 = 0x0003,
    Floating32 = 0x0004,
    Floating64 = 0x0005,
    Error = 0x000A,
    Boolean = 0x000B,
    Integer64 = 0x0014,
    String8 = 0x001E,
    String = 0x001F,
    Time = 0x0040,
    Guid = 0x0048,
    ServerId = 0x00FB,
    Binary = 0x0102,
    MultipleInteger16 = 0x1002,
    MultipleInteger32 = 0x1003,
    MultipleInteger64 = 0x1014,
    MultipleString8 = 0x101E,
    MultipleString = 0x101F,
    MultipleGuid = 0x1048,
    MultipleBinary = 0x1102,
}

impl MapiPropertyType {
    #[allow(dead_code)]
    pub(in crate::mapi) const fn as_u16(self) -> u16 {
        self as u16
    }

    pub(in crate::mapi) fn from_code(value: u16) -> Option<Self> {
        match value {
            0x0002 => Some(Self::Integer16),
            0x0003 => Some(Self::Integer32),
            0x0004 => Some(Self::Floating32),
            0x0005 => Some(Self::Floating64),
            0x000A => Some(Self::Error),
            0x000B => Some(Self::Boolean),
            0x0014 => Some(Self::Integer64),
            0x001E => Some(Self::String8),
            0x001F => Some(Self::String),
            0x0040 => Some(Self::Time),
            0x0048 => Some(Self::Guid),
            0x00FB => Some(Self::ServerId),
            0x0102 => Some(Self::Binary),
            0x1002 => Some(Self::MultipleInteger16),
            0x1003 => Some(Self::MultipleInteger32),
            0x1014 => Some(Self::MultipleInteger64),
            0x101E => Some(Self::MultipleString8),
            0x101F => Some(Self::MultipleString),
            0x1048 => Some(Self::MultipleGuid),
            0x1102 => Some(Self::MultipleBinary),
            _ => {
                let known_unsupported_name = Self::known_unsupported_name(value);
                tracing::warn!(
                    adapter = "mapi",
                    enum_name = "MapiPropertyType",
                    raw_value = value,
                    known_unsupported = known_unsupported_name.is_some(),
                    known_unsupported_name = known_unsupported_name.unwrap_or(""),
                    "unsupported MAPI property type"
                );
                None
            }
        }
    }

    #[allow(dead_code)]
    pub(in crate::mapi) fn known_unsupported_name(value: u16) -> Option<&'static str> {
        match value {
            0x0000 => Some("PtypUnspecified"),
            0x0001 => Some("PtypNull"),
            0x0006 => Some("PtypCurrency"),
            0x0007 => Some("PtypFloatingTime"),
            0x000D => Some("PtypObject"),
            0x00FD => Some("PtypRestriction"),
            0x00FE => Some("PtypRuleAction"),
            0x1004 => Some("PtypMultipleFloating32"),
            0x1005 => Some("PtypMultipleFloating64"),
            0x1006 => Some("PtypMultipleCurrency"),
            0x1007 => Some("PtypMultipleFloatingTime"),
            0x10FB => Some("PtypMultipleServerId"),
            0x10FD => Some("PtypMultipleRestriction"),
            0x10FE => Some("PtypMultipleRuleAction"),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(in crate::mapi) enum MapiRestrictionType {
    And = 0x00,
    Or = 0x01,
    Not = 0x02,
    Content = 0x03,
    Property = 0x04,
    CompareProperties = 0x05,
    Bitmask = 0x06,
    Size = 0x07,
    Exist = 0x08,
    SubObject = 0x09,
    Comment = 0x0A,
    Count = 0x0B,
}

impl MapiRestrictionType {
    pub(in crate::mapi) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x00 => Some(Self::And),
            0x01 => Some(Self::Or),
            0x02 => Some(Self::Not),
            0x03 => Some(Self::Content),
            0x04 => Some(Self::Property),
            0x05 => Some(Self::CompareProperties),
            0x06 => Some(Self::Bitmask),
            0x07 => Some(Self::Size),
            0x08 => Some(Self::Exist),
            0x09 => Some(Self::SubObject),
            0x0A => Some(Self::Comment),
            0x0B => Some(Self::Count),
            _ => {
                tracing::warn!(
                    adapter = "mapi",
                    enum_name = "MapiRestrictionType",
                    raw_value = value,
                    "unsupported MAPI restriction type"
                );
                None
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
#[allow(dead_code)]
pub(crate) enum MapiSyncType {
    Contents = 0x01,
    Hierarchy = 0x02,
    ReadState = 0x03,
}

impl MapiSyncType {
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    #[allow(dead_code)]
    pub(crate) fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Contents),
            0x02 => Some(Self::Hierarchy),
            0x03 => Some(Self::ReadState),
            _ => {
                tracing::warn!(
                    adapter = "mapi",
                    enum_name = "MapiSyncType",
                    raw_value = value,
                    "unsupported MAPI synchronization type"
                );
                None
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub(crate) enum FastTransferMarker {
    IncrSyncChg = 0x4012_0003,
    IncrSyncDel = 0x4013_0003,
    IncrSyncEnd = 0x4014_0003,
    IncrSyncMessage = 0x4015_0003,
    IncrSyncRead = 0x402F_0003,
    IncrSyncStateBegin = 0x403A_0003,
    IncrSyncStateEnd = 0x403B_0003,
}

impl FastTransferMarker {
    pub(crate) const fn as_u32(self) -> u32 {
        self as u32
    }

    #[allow(dead_code)]
    pub(crate) fn from_u32(value: u32) -> Option<Self> {
        match value {
            0x4012_0003 => Some(Self::IncrSyncChg),
            0x4013_0003 => Some(Self::IncrSyncDel),
            0x4014_0003 => Some(Self::IncrSyncEnd),
            0x4015_0003 => Some(Self::IncrSyncMessage),
            0x402F_0003 => Some(Self::IncrSyncRead),
            0x403A_0003 => Some(Self::IncrSyncStateBegin),
            0x403B_0003 => Some(Self::IncrSyncStateEnd),
            _ => {
                let known_unsupported_name = Self::known_unsupported_name(value);
                tracing::warn!(
                    adapter = "mapi",
                    enum_name = "FastTransferMarker",
                    raw_value = value,
                    known_unsupported = known_unsupported_name.is_some(),
                    known_unsupported_name = known_unsupported_name.unwrap_or(""),
                    "unsupported MAPI FastTransfer marker"
                );
                None
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn known_unsupported_name(value: u32) -> Option<&'static str> {
        match value {
            0x4009_0003 => Some("StartTopFld"),
            0x400A_0003 => Some("StartSubFld"),
            0x400B_0003 => Some("EndFolder"),
            0x400C_0003 => Some("StartMessage"),
            0x400D_0003 => Some("EndMessage"),
            0x4010_0003 => Some("StartFAIMsg"),
            0x4001_0003 => Some("StartEmbed"),
            0x4002_0003 => Some("EndEmbed"),
            0x4003_0003 => Some("StartRecip"),
            0x4004_0003 => Some("EndToRecip"),
            0x4000_0003 => Some("NewAttach"),
            0x400E_0003 => Some("EndAttach"),
            0x407D_0003 => Some("IncrSyncChgPartial"),
            0x4074_000B => Some("IncrSyncProgressMode"),
            0x4075_000B => Some("IncrSyncProgressPerMsg"),
            0x407B_0102 => Some("IncrSyncGroupInfo"),
            0x4018_0003 => Some("FXErrorInfo"),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
#[allow(dead_code)]
pub(in crate::mapi) enum MapiNotificationEventMask {
    CriticalError = 0x0001,
    NewMail = 0x0002,
    ObjectCreated = 0x0004,
    ObjectDeleted = 0x0008,
    ObjectModified = 0x0010,
    ObjectMoved = 0x0020,
    TableModified = 0x0100,
    Extended = 0x0400,
}

impl MapiNotificationEventMask {
    pub(in crate::mapi) const fn as_u16(self) -> u16 {
        self as u16
    }
}

pub(in crate::mapi) const MAPI_CONTENT_NOTIFICATION_MASK: u16 = MapiNotificationEventMask::NewMail
    .as_u16()
    | MapiNotificationEventMask::ObjectCreated.as_u16()
    | MapiNotificationEventMask::ObjectDeleted.as_u16()
    | MapiNotificationEventMask::ObjectModified.as_u16()
    | MapiNotificationEventMask::ObjectMoved.as_u16()
    | MapiNotificationEventMask::TableModified.as_u16();

pub(in crate::mapi) const MAPI_HIERARCHY_NOTIFICATION_MASK: u16 =
    MapiNotificationEventMask::ObjectCreated.as_u16()
        | MapiNotificationEventMask::ObjectDeleted.as_u16()
        | MapiNotificationEventMask::ObjectModified.as_u16()
        | MapiNotificationEventMask::ObjectMoved.as_u16()
        | MapiNotificationEventMask::TableModified.as_u16();

#[allow(dead_code)]
pub(in crate::mapi) const MAPI_SUPPORTED_NOTIFICATION_MASK: u16 =
    MapiNotificationEventMask::CriticalError.as_u16()
        | MapiNotificationEventMask::NewMail.as_u16()
        | MapiNotificationEventMask::ObjectCreated.as_u16()
        | MapiNotificationEventMask::ObjectDeleted.as_u16()
        | MapiNotificationEventMask::ObjectModified.as_u16()
        | MapiNotificationEventMask::ObjectMoved.as_u16()
        | MapiNotificationEventMask::TableModified.as_u16()
        | MapiNotificationEventMask::Extended.as_u16();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
#[allow(dead_code)]
pub(in crate::mapi) enum MapiError {
    Success = 0x0000_0000,
    NotFound = 0x8004_010F,
    InvalidParameter = 0x8004_0102,
    NoAccess = 0x8007_0005,
    NotEnoughMemory = 0x8007_000E,
    CallFailed = 0x8004_0405,
    InvalidFunction = 0x8007_0057,
    NoSupport = 0x0000_04B9,
}

impl MapiError {
    pub(in crate::mapi) const fn as_u32(self) -> u32 {
        self as u32
    }
}

#[cfg(test)]
mod tests {
    use crate::microsoft_protocol_audit::{
        gap_status_u16, gap_status_u32, gap_status_u8, GapStatus,
        FAST_TRANSFER_MARKER_GAP_MANIFEST, PROPERTY_TYPE_GAP_MANIFEST, ROP_ID_GAP_MANIFEST,
    };

    use super::*;

    #[test]
    fn typed_wire_values_match_documented_constants() {
        for (value, request_type) in MapiHttpRequestType::DOCUMENTED_SUPPORTED_VALUES {
            assert_eq!(request_type.header_value(), *value);
        }
        assert_eq!(RopId::GetStatus.as_u8(), 0x16);
        assert_eq!(RopId::QueryPosition.as_u8(), 0x17);
        assert_eq!(RopId::SeekRow.as_u8(), 0x18);
        assert_eq!(RopId::CreateBookmark.as_u8(), 0x1B);
        assert_eq!(RopId::Abort.as_u8(), 0x38);
        assert_eq!(RopId::CopyTo.as_u8(), 0x39);
        assert_eq!(RopId::OpenEmbeddedMessage.as_u8(), 0x46);
        assert_eq!(RopId::SetSpooler.as_u8(), 0x47);
        assert_eq!(RopId::SpoolerLockMessage.as_u8(), 0x48);
        assert_eq!(RopId::ExpandRow.as_u8(), 0x59);
        assert_eq!(RopId::CollapseRow.as_u8(), 0x5A);
        assert_eq!(RopId::LockRegionStream.as_u8(), 0x5B);
        assert_eq!(RopId::UnlockRegionStream.as_u8(), 0x5C);
        assert_eq!(RopId::CommitStream.as_u8(), 0x5D);
        assert_eq!(RopId::SynchronizationImportReadStateChanges.as_u8(), 0x80);
        assert_eq!(RopId::SynchronizationGetTransferState.as_u8(), 0x82);
        assert_eq!(RopId::TellVersion.as_u8(), 0x86);
        assert_eq!(RopId::FreeBookmark.as_u8(), 0x89);
        assert_eq!(RopId::WriteAndCommitStream.as_u8(), 0x90);
        assert_eq!(RopId::HardDeleteMessages.as_u8(), 0x91);
        assert_eq!(RopId::HardDeleteMessagesAndSubfolders.as_u8(), 0x92);
        assert_eq!(RopId::SetLocalReplicaMidsetDeleted.as_u8(), 0x93);
        assert_eq!(
            RopId::FastTransferDestinationPutBufferExtended.as_u8(),
            0x9D
        );
        assert_eq!(RopId::SynchronizationConfigure.as_u8(), 0x70);
        assert_eq!(RopId::Logon.as_u8(), 0xFE);
        assert_eq!(MapiPropertyType::Boolean.as_u16(), 0x000B);
        assert_eq!(MapiRestrictionType::Exist as u8, 0x08);
        assert_eq!(MapiSyncType::Contents.as_u8(), 0x01);
        assert_eq!(FastTransferMarker::IncrSyncStateEnd.as_u32(), 0x403B_0003);
        assert_eq!(MapiNotificationEventMask::TableModified.as_u16(), 0x0100);
        assert_eq!(MapiError::NotFound.as_u32(), 0x8004_010F);
    }

    #[test]
    fn typed_wire_values_decode_known_values_only() {
        for value in 0..=u8::MAX {
            if let Some(rop_id) = RopId::from_u8(value) {
                assert_eq!(rop_id.as_u8(), value);
                assert!(!RopId::is_reserved(value) || rop_id == RopId::Reserved);
                assert!(
                    gap_status_u8(ROP_ID_GAP_MANIFEST, value).is_some(),
                    "RopId 0x{value:02X} is decoded but missing from the gap manifest"
                );
            }
        }

        let property_types = [
            MapiPropertyType::Integer16,
            MapiPropertyType::Integer32,
            MapiPropertyType::Floating32,
            MapiPropertyType::Floating64,
            MapiPropertyType::Error,
            MapiPropertyType::Boolean,
            MapiPropertyType::Integer64,
            MapiPropertyType::String8,
            MapiPropertyType::String,
            MapiPropertyType::Time,
            MapiPropertyType::Guid,
            MapiPropertyType::ServerId,
            MapiPropertyType::Binary,
            MapiPropertyType::MultipleInteger16,
            MapiPropertyType::MultipleInteger32,
            MapiPropertyType::MultipleInteger64,
            MapiPropertyType::MultipleString8,
            MapiPropertyType::MultipleString,
            MapiPropertyType::MultipleGuid,
            MapiPropertyType::MultipleBinary,
        ];
        for property_type in property_types {
            assert_eq!(
                MapiPropertyType::from_code(property_type.as_u16()),
                Some(property_type)
            );
            assert_eq!(
                gap_status_u16(PROPERTY_TYPE_GAP_MANIFEST, property_type.as_u16()),
                Some(GapStatus::Implemented)
            );
        }

        let markers = [
            FastTransferMarker::IncrSyncChg,
            FastTransferMarker::IncrSyncDel,
            FastTransferMarker::IncrSyncEnd,
            FastTransferMarker::IncrSyncMessage,
            FastTransferMarker::IncrSyncRead,
            FastTransferMarker::IncrSyncStateBegin,
            FastTransferMarker::IncrSyncStateEnd,
        ];
        for marker in markers {
            assert_eq!(FastTransferMarker::from_u32(marker.as_u32()), Some(marker));
            assert_eq!(
                gap_status_u32(FAST_TRANSFER_MARKER_GAP_MANIFEST, marker.as_u32()),
                Some(GapStatus::Implemented)
            );
        }

        assert_eq!(RopId::from_u8(0x02), Some(RopId::OpenFolder));
        assert_eq!(RopId::from_u8(0x39), Some(RopId::CopyTo));
        assert_eq!(
            RopId::from_u8(0x9D),
            Some(RopId::FastTransferDestinationPutBufferExtended)
        );
        assert_eq!(RopId::from_u8(0xA3), Some(RopId::WriteStreamExtended));
        assert_eq!(RopId::from_u8(0xAA), None);
        assert!(RopId::is_reserved(0x28));
        assert!(!RopId::is_reserved(0x70));
        assert_eq!(RopId::known_unsupported_name(0x34), None);
        assert_eq!(RopId::known_unsupported_name(0x39), Some("RopCopyTo"));
        assert_eq!(
            RopId::known_unsupported_name(0x51),
            Some("RopTransportNewMail")
        );
        assert_eq!(
            MapiPropertyType::from_code(0x001F),
            Some(MapiPropertyType::String)
        );
        assert_eq!(MapiPropertyType::from_code(0x000D), None);
        assert_eq!(
            MapiPropertyType::known_unsupported_name(0x000D),
            Some("PtypObject")
        );
        assert_eq!(
            FastTransferMarker::from_u32(0x4012_0003),
            Some(FastTransferMarker::IncrSyncChg)
        );
        assert_eq!(
            FastTransferMarker::known_unsupported_name(0x4009_0003),
            Some("StartTopFld")
        );
        assert_eq!(
            FastTransferMarker::known_unsupported_name(0x4074_000B),
            Some("IncrSyncProgressMode")
        );
        assert_eq!(
            FastTransferMarker::known_unsupported_name(0x4075_000B),
            Some("IncrSyncProgressPerMsg")
        );
        assert_eq!(
            FastTransferMarker::known_unsupported_name(0x407B_0102),
            Some("IncrSyncGroupInfo")
        );
        assert_eq!(
            FastTransferMarker::known_unsupported_name(0x4018_0003),
            Some("FXErrorInfo")
        );
        assert_eq!(FastTransferMarker::from_u32(0xDEAD_BEEF), None);
        assert_eq!(
            MapiRestrictionType::from_u8(0x03),
            Some(MapiRestrictionType::Content)
        );
        assert_eq!(MapiSyncType::from_u8(0x03), Some(MapiSyncType::ReadState));
    }
}
