#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GapStatus {
    Implemented,
    KnownUnsupported,
    Reserved,
    NotInScope,
}

impl GapStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Implemented => "Implemented",
            Self::KnownUnsupported => "KnownUnsupported",
            Self::Reserved => "Reserved",
            Self::NotInScope => "NotInScope",
        }
    }
}

struct ReportRow {
    surface: &'static str,
    spec: &'static str,
    source: &'static str,
    anchor: &'static str,
    implemented: &'static str,
    gaps: &'static str,
}

pub(crate) const ROP_ID_GAP_MANIFEST: &[(u8, GapStatus)] = &[
    (0x01, GapStatus::Implemented),
    (0x02, GapStatus::Implemented),
    (0x03, GapStatus::Implemented),
    (0x04, GapStatus::Implemented),
    (0x05, GapStatus::Implemented),
    (0x06, GapStatus::Implemented),
    (0x07, GapStatus::Implemented),
    (0x08, GapStatus::Implemented),
    (0x09, GapStatus::Implemented),
    (0x0A, GapStatus::Implemented),
    (0x0B, GapStatus::Implemented),
    (0x0C, GapStatus::Implemented),
    (0x0D, GapStatus::Implemented),
    (0x0E, GapStatus::Implemented),
    (0x0F, GapStatus::Implemented),
    (0x10, GapStatus::Implemented),
    (0x11, GapStatus::Implemented),
    (0x12, GapStatus::Implemented),
    (0x13, GapStatus::Implemented),
    (0x14, GapStatus::Implemented),
    (0x15, GapStatus::Implemented),
    (0x16, GapStatus::Implemented),
    (0x17, GapStatus::Implemented),
    (0x18, GapStatus::Implemented),
    (0x19, GapStatus::Implemented),
    (0x1A, GapStatus::Implemented),
    (0x1B, GapStatus::Implemented),
    (0x1C, GapStatus::Implemented),
    (0x1D, GapStatus::Implemented),
    (0x1E, GapStatus::Implemented),
    (0x1F, GapStatus::Implemented),
    (0x20, GapStatus::Implemented),
    (0x21, GapStatus::Implemented),
    (0x22, GapStatus::Implemented),
    (0x23, GapStatus::Implemented),
    (0x24, GapStatus::Implemented),
    (0x25, GapStatus::Implemented),
    (0x26, GapStatus::Implemented),
    (0x27, GapStatus::Implemented),
    (0x28, GapStatus::Reserved),
    (0x29, GapStatus::Implemented),
    (0x2A, GapStatus::NotInScope),
    (0x2B, GapStatus::Implemented),
    (0x2C, GapStatus::Implemented),
    (0x2D, GapStatus::Implemented),
    (0x2E, GapStatus::Implemented),
    (0x2F, GapStatus::Implemented),
    (0x30, GapStatus::Implemented),
    (0x31, GapStatus::Implemented),
    (0x32, GapStatus::Implemented),
    (0x33, GapStatus::Implemented),
    (0x34, GapStatus::Implemented),
    (0x35, GapStatus::Implemented),
    (0x36, GapStatus::Implemented),
    (0x37, GapStatus::Implemented),
    (0x38, GapStatus::Implemented),
    (0x39, GapStatus::Implemented),
    (0x3A, GapStatus::Implemented),
    (0x3B, GapStatus::Implemented),
    (0x3E, GapStatus::Implemented),
    (0x3F, GapStatus::Implemented),
    (0x40, GapStatus::Implemented),
    (0x41, GapStatus::Implemented),
    (0x42, GapStatus::Implemented),
    (0x43, GapStatus::Implemented),
    (0x44, GapStatus::Implemented),
    (0x45, GapStatus::Implemented),
    (0x46, GapStatus::Implemented),
    (0x47, GapStatus::Implemented),
    (0x48, GapStatus::Implemented),
    (0x49, GapStatus::Implemented),
    (0x4A, GapStatus::Implemented),
    (0x4B, GapStatus::Implemented),
    (0x4C, GapStatus::Implemented),
    (0x4D, GapStatus::Implemented),
    (0x4E, GapStatus::Implemented),
    (0x4F, GapStatus::Implemented),
    (0x50, GapStatus::Implemented),
    (0x51, GapStatus::Implemented),
    (0x52, GapStatus::Implemented),
    (0x53, GapStatus::Implemented),
    (0x54, GapStatus::Implemented),
    (0x55, GapStatus::Implemented),
    (0x56, GapStatus::Implemented),
    (0x57, GapStatus::Implemented),
    (0x58, GapStatus::Implemented),
    (0x59, GapStatus::Implemented),
    (0x5A, GapStatus::Implemented),
    (0x5B, GapStatus::Implemented),
    (0x5C, GapStatus::Implemented),
    (0x5D, GapStatus::Implemented),
    (0x5E, GapStatus::Implemented),
    (0x5F, GapStatus::Implemented),
    (0x60, GapStatus::Implemented),
    (0x61, GapStatus::Implemented),
    (0x63, GapStatus::Implemented),
    (0x64, GapStatus::Implemented),
    (0x66, GapStatus::Implemented),
    (0x67, GapStatus::Implemented),
    (0x68, GapStatus::Implemented),
    (0x69, GapStatus::Implemented),
    (0x6B, GapStatus::Implemented),
    (0x6C, GapStatus::Implemented),
    (0x6D, GapStatus::Implemented),
    (0x6E, GapStatus::NotInScope),
    (0x6F, GapStatus::Implemented),
    (0x70, GapStatus::Implemented),
    (0x72, GapStatus::Implemented),
    (0x73, GapStatus::Implemented),
    (0x74, GapStatus::Implemented),
    (0x75, GapStatus::Implemented),
    (0x76, GapStatus::Implemented),
    (0x77, GapStatus::Implemented),
    (0x78, GapStatus::Implemented),
    (0x79, GapStatus::Implemented),
    (0x7A, GapStatus::Implemented),
    (0x7B, GapStatus::Implemented),
    (0x7E, GapStatus::Implemented),
    (0x7F, GapStatus::Implemented),
    (0x80, GapStatus::Implemented),
    (0x81, GapStatus::Implemented),
    (0x82, GapStatus::Implemented),
    (0x83, GapStatus::Implemented),
    (0x84, GapStatus::Implemented),
    (0x85, GapStatus::Implemented),
    (0x86, GapStatus::Implemented),
    (0x87, GapStatus::Implemented),
    (0x89, GapStatus::Implemented),
    (0x90, GapStatus::Implemented),
    (0x91, GapStatus::Implemented),
    (0x92, GapStatus::Implemented),
    (0x93, GapStatus::Implemented),
    (0x9D, GapStatus::Implemented),
    (0xA3, GapStatus::Implemented),
    (0xFE, GapStatus::Implemented),
];

pub(crate) const PROPERTY_TYPE_GAP_MANIFEST: &[(u16, GapStatus)] = &[
    (0x0000, GapStatus::KnownUnsupported),
    (0x0001, GapStatus::KnownUnsupported),
    (0x0002, GapStatus::Implemented),
    (0x0003, GapStatus::Implemented),
    (0x0004, GapStatus::Implemented),
    (0x0005, GapStatus::Implemented),
    (0x0006, GapStatus::KnownUnsupported),
    (0x0007, GapStatus::KnownUnsupported),
    (0x000A, GapStatus::Implemented),
    (0x000B, GapStatus::Implemented),
    (0x000D, GapStatus::KnownUnsupported),
    (0x0014, GapStatus::Implemented),
    (0x001E, GapStatus::Implemented),
    (0x001F, GapStatus::Implemented),
    (0x0040, GapStatus::Implemented),
    (0x0048, GapStatus::Implemented),
    (0x00FB, GapStatus::Implemented),
    (0x00FD, GapStatus::KnownUnsupported),
    (0x00FE, GapStatus::KnownUnsupported),
    (0x0102, GapStatus::Implemented),
    (0x1002, GapStatus::Implemented),
    (0x1003, GapStatus::Implemented),
    (0x1004, GapStatus::KnownUnsupported),
    (0x1005, GapStatus::KnownUnsupported),
    (0x1006, GapStatus::KnownUnsupported),
    (0x1007, GapStatus::KnownUnsupported),
    (0x1014, GapStatus::Implemented),
    (0x101E, GapStatus::Implemented),
    (0x101F, GapStatus::Implemented),
    (0x1040, GapStatus::Implemented),
    (0x1048, GapStatus::Implemented),
    (0x10FB, GapStatus::KnownUnsupported),
    (0x10FD, GapStatus::KnownUnsupported),
    (0x10FE, GapStatus::KnownUnsupported),
    (0x1102, GapStatus::Implemented),
];

pub(crate) const FAST_TRANSFER_MARKER_GAP_MANIFEST: &[(u32, GapStatus)] = &[
    (0x4000_0003, GapStatus::Implemented),
    (0x4001_0003, GapStatus::Implemented),
    (0x4002_0003, GapStatus::Implemented),
    (0x4003_0003, GapStatus::Implemented),
    (0x4004_0003, GapStatus::Implemented),
    (0x4009_0003, GapStatus::Implemented),
    (0x400A_0003, GapStatus::Implemented),
    (0x400B_0003, GapStatus::Implemented),
    (0x400C_0003, GapStatus::Implemented),
    (0x400D_0003, GapStatus::Implemented),
    (0x400E_0003, GapStatus::Implemented),
    (0x4010_0003, GapStatus::Implemented),
    (0x4012_0003, GapStatus::Implemented),
    (0x4013_0003, GapStatus::Implemented),
    (0x4014_0003, GapStatus::Implemented),
    (0x4015_0003, GapStatus::Implemented),
    (0x4018_0003, GapStatus::KnownUnsupported),
    (0x402F_0003, GapStatus::Implemented),
    (0x403A_0003, GapStatus::Implemented),
    (0x403B_0003, GapStatus::Implemented),
    (0x4074_000B, GapStatus::Implemented),
    (0x4075_000B, GapStatus::Implemented),
    (0x407B_0102, GapStatus::KnownUnsupported),
    (0x407D_0003, GapStatus::KnownUnsupported),
];

const REPORT_ROWS: &[ReportRow] = &[
    ReportRow {
        surface: "ActiveSync command codes",
        spec: "MS-ASHTTP command codes",
        source: "https://learn.microsoft.com/pl-pl/openspecs/exchange_server_protocols/ms-ashttp/0ab55ebc-6ea9-4ae4-af37-5736d5195d46",
        anchor: "`crates/lpe-activesync/src/protocol.rs::activesync_command_codes_match_ms_ashttp`",
        implemented: "`Sync`, `SendMail`, `SmartForward`, `SmartReply`, `GetAttachment`, `FolderSync`, `FolderCreate`, `FolderDelete`, `FolderUpdate`, `MoveItems`, `GetItemEstimate`, `MeetingResponse`, `Search`, `Settings`, `Ping`, `ItemOperations`, `Provision`, `ResolveRecipients`, `ValidateCert`, `Find` command-code constants are covered.",
        gaps: "Runtime support remains bounded by the ActiveSync service handlers; `GetAttachment`, `MeetingResponse`, `Settings`, `ResolveRecipients`, and `ValidateCert` are named as known unsupported where applicable.",
    },
    ReportRow {
        surface: "ActiveSync WBXML code pages",
        spec: "MS-ASWBXML code pages",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-aswbxml/bc835874-7de1-452c-abd5-de5e4709626c",
        anchor: "`crates/lpe-activesync/src/protocol.rs::wbxml_code_pages_match_bounded_ms_aswbxml_manifest`",
        implemented: "AirSync, Contacts, Email, Calendar, Tasks, Move, GetItemEstimate, FolderHierarchy, Ping, Provision, Search, AirSyncBase, Settings, ItemOperations, ComposeMail.",
        gaps: "`AirNotify`, `MeetingResponse`, `ResolveRecipients`, `ValidateCert`, `Contacts2`, `GAL`, `DocumentLibrary`, `Email2`, `Notes`, `RightsManagement`, `Find`.",
    },
    ReportRow {
        surface: "ActiveSync status/folder/body values",
        spec: "MS-ASCMD / MS-ASWBXML simple values",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-ascmd/0b93d908-d1dc-412c-87b0-cb70d3c95651",
        anchor: "`crates/lpe-activesync/src/protocol.rs::active_sync_status_folder_and_body_values_are_manifest_checked`",
        implemented: "Implemented status codes, folder type IDs, and body preference values are manifest-checked.",
        gaps: "Additional command-specific status spaces are not exhaustively represented unless LPE emits them.",
    },
    ReportRow {
        surface: "MAPIHTTP request types",
        spec: "MS-OXCMAPIHTTP `X-RequestType`",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmapihttp/cb1f2c87-eb69-418f-9e59-c30c179615a0",
        anchor: "`crates/lpe-exchange/src/mapi/wire.rs::DOCUMENTED_SUPPORTED_VALUES`",
        implemented: "EMSMDB and NSPI request types implemented by LPE are manifest-checked. MS-OXCMAPIHTTP 4.x protocol examples are covered by `crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxcmapihttp_connect_execute_reconnect_disconnect_sequence` and `mapi_over_http_microsoft_oxcmapihttp_ping_refreshes_idle_session_context` for Connect, Execute, Reconnect, Disconnect, session cookies, request/client echo headers, response code, pending/expiration headers, and idle-session Ping refresh.",
        gaps: "Unknown request types remain `Unsupported(String)` diagnostics. Coverage is bounded to the EMSMDB and NSPI request types that LPE exposes for Outlook interoperability.",
    },
    ReportRow {
        surface: "ROP IDs",
        spec: "MS-OXCROPS RopId table",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcrops/6c623489-576d-45ef-9288-5b62b73c6961",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::ROP_ID_GAP_MANIFEST`, `crates/lpe-exchange/src/mapi/rop.rs::tests::microsoft_oxcrops_rop_buffer_request_examples_parse_expected_fields`, `buffer_too_small_response_matches_microsoft_rop_layout`, `backoff_response_matches_microsoft_logon_example`, `backoff_response_matches_microsoft_targeted_rop_example`",
        implemented: "Every `RopId::from_u8` decoded value must be classified by test. MS-OXCROPS 4.1 through 4.4 ROP-buffer request examples are parsed for empty buffers, single `QueryRows`, chained `OpenFolder`/`GetHierarchyTable`, `Release`, and server-object handle tables. MS-OXCROPS 4.5 through 4.7 response examples are covered for `RopBufferTooSmall` and both logon-level and targeted `RopBackoff` layouts.",
        gaps: "Selected decoded values are `KnownUnsupported` or `NotInScope`, including notify/pending and reserved entries. `RopRequest` does not retain `LogonId` as a first-class field for test serialization, so request parser tests verify modeled fields and handle-table layout rather than exact nonzero-LogonId request reserialization.",
    },
    ReportRow {
        surface: "MAPI table ROP examples",
        spec: "MS-OXCTABL table object examples",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxctabl/",
        anchor: "`crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxctabl_4_1_to_4_4_contents_table_setcolumns_sort_query_rows`, `mapi_over_http_microsoft_categorized_table_sort_query_and_expand_rows`",
        implemented: "MS-OXCTABL 4.1 through 4.4 are covered with an Inbox contents table using the documented `GetContentsTable`, six-column `SetColumns` list (`PidTagFolderId`, `PidTagMid`, `PidTagInstID`, `PidTagInstanceNum`, `PidTagSubject`, `PidTagMessageDeliveryTime`), descending delivery-time `SortTable`, and `QueryRows`. MS-OXCTABL 4.5 category sorting, collapsed query, `ExpandRow`, and expanded query rows are covered by the categorized table test.",
        gaps: "Coverage is bounded to contents tables and categorized mail views modeled by LPE; arbitrary Exchange table providers and view-designer permutations remain outside the current interoperability claim.",
    },
    ReportRow {
        surface: "Message and attachment object examples",
        spec: "MS-OXCMSG message and attachment object examples",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmsg/",
        anchor: "`crates/lpe-exchange/src/mapi/rop.rs::tests::microsoft_oxcmsg_core_request_examples_parse_expected_fields`, `microsoft_oxcmsg_attachment_request_examples_parse_expected_fields`, `microsoft_oxcmsg_modify_recipients_example_parses_wrapped_recipient_row`, `crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email`, `mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object`, `mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save`, `mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment`, `mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body`, `mapi_over_http_microsoft_modify_recipients_example_saves_canonically`",
        implemented: "MS-OXCMSG 4.1 through 4.8 are covered by typed parser golden examples plus MAPI/HTTP integration tests for `CreateMessage`, named-property mapping, `GetAttachmentTable`, inline HTML image attachment creation/properties/stream/save, text-file attachment creation/properties/stream/save, message property save including HTML CID preservation, `ModifyRecipients`, and `SaveChangesMessage` importing canonical email and attachments.",
        gaps: "Coverage is bounded to mail messages and file/inline attachments modeled by LPE; full Exchange attachment provider breadth and every optional message class remain outside the current interoperability claim.",
    },
    ReportRow {
        surface: "Folder object examples",
        spec: "MS-OXCFOLD folder object examples",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfold/",
        anchor: "`crates/lpe-exchange/src/mapi/rop.rs::tests::microsoft_oxcfold_create_and_hierarchy_examples_parse_through_typed_parser`, `microsoft_oxcfold_folder_mutation_examples_parse_expected_fields`, `microsoft_oxcfold_folder_move_copy_and_search_examples_parse_expected_fields`, `microsoft_oxcfold_set_search_criteria_example_parses_scope_and_flags`, `crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxcfold_folder_examples_use_canonical_mailboxes`, `mapi_over_http_microsoft_delete_messages_uses_trash_and_hard_delete`, `mapi_over_http_microsoft_move_copy_messages_accepts_nonzero_boolean_fields`, `mapi_over_http_microsoft_folder_search_criteria_example_round_trips_message_class_and_importance`",
        implemented: "MS-OXCFOLD 4.1 through 4.9 request examples are covered by typed parser golden tests for create/delete folder, delete/move messages, move/copy folder, hierarchy table, set search criteria, and get search criteria. MAPI/HTTP tests cover canonical mailbox creation/deletion/move/copy, message delete/move/copy behavior, hierarchy table row projection, and search criteria round-tripping for the documented message-class exclusion plus high-importance restriction.",
        gaps: "Coverage is bounded to private mailbox folders and the public-folder operations modeled by LPE; full Exchange managed-folder policy behavior and arbitrary search-folder providers remain outside the current interoperability claim.",
    },
    ReportRow {
        surface: "MAPI property types",
        spec: "MS-OXCDATA property data types",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcdata/0c77892e-288e-435a-9c49-be1c20c7afdb",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::PROPERTY_TYPE_GAP_MANIFEST`",
        implemented: "Implemented scalar and multivalue property types are manifest-checked. `RopGetPropertiesSpecific` projects `PtypUnspecified` requests as typed values for modeled object properties, including the MS-OXCDATA PropertyRow example. `PtypServerId` is encoded as the same counted byte-vector shape used by `PtypBinary`; `PtypMultipleTime` is encoded as counted FILETIME values.",
        gaps: "`PtypUnspecified` requests for properties not modeled by LPE still return normal property errors. `PtypNull`, currency, floating-time, object, restriction, ruleAction, and related multivalue forms are explicitly `KnownUnsupported`.",
    },
    ReportRow {
        surface: "FastTransfer/ICS markers",
        spec: "MS-OXCFXICS markers",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfxics/e8e45630-33dd-4974-84d0-7b68a037a724",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::FAST_TRANSFER_MARKER_GAP_MANIFEST`",
        implemented: "Incremental sync markers used by LPE, normal/FAI message markers, recipient/attachment markers, embedded-message markers, CopyFolder folder markers, and the MS-OXCFXICS REPLID/GLOBSET IDSET serialization example are manifest-checked. `RopSynchronizationConfigure` names `PartialItem` and logs the documented full-item fallback when partial message changes are not implemented. The MS-OXCFXICS 4.1.1 hierarchy upload example is covered through `SynchronizationOpenCollector`, `UploadStateStreamBegin`/`End`, `ImportHierarchyChange`, `GetTransferState`, and canonical mailbox creation. The MS-OXCFXICS 4.1.2 hierarchy delete example is covered through `SynchronizationOpenCollector`, `UploadStateStreamBegin`/`End`, `ImportDeletes`, `GetTransferState`, and canonical mailbox deletion. The MS-OXCFXICS 4.2.1 message upload example is covered through content `SynchronizationOpenCollector`, `CnsetSeen`/`CnsetSeenFAI`/`CnsetRead` upload-state streams, `ImportMessageChange`, `SaveChangesMessage`, `GetTransferState`, preserved source-key identity, and canonical email import. The MS-OXCFXICS 4.2.2 message delete example is covered through content `SynchronizationOpenCollector`, `CnsetSeen`/`CnsetSeenFAI`/`CnsetRead` upload-state streams, `ImportDeletes`, `GetTransferState`, and canonical hard-delete propagation into upload ICS state. The MS-OXCFXICS 4.3.1 partial item upload example is covered through existing-message `DeletePropertiesNoReplicate`/`SetPropertiesNoReplicate`/`SaveChangesMessage` and `GetTransferState`. The MS-OXCFXICS 4.3.2 partial item download example is covered through `SynchronizationConfigure` with `PartialItem`, `IdsetGiven`/`CnsetSeen`/`CnsetSeenFAI`/`CnsetRead` upload-state streams, `FastTransferSourceGetBuffer`, and LPE's documented full-item fallback. The MS-OXCFXICS 4.5 content synchronization download example is covered end-to-end through Outlook-style upload-state, progress, message, recipient, embedded attachment, deletion, read-state, final-state, and end markers. The MS-OXCFXICS 4.6 conflict examples are covered for existing-message content uploads with `FailOnConflict` and `PidTagPredecessorChangeList` comparison against the current server change key.",
        gaps: "Property-group partial-change markers remain explicitly `KnownUnsupported` (`IncrSyncGroupInfo`, `IncrSyncChgPartial`) because LPE currently uses the MS-OXCFXICS full-item fallback. `FXErrorInfo` is also `KnownUnsupported`.",
    },
    ReportRow {
        surface: "Outlook configuration and view FAI",
        spec: "MS-OXOCFG configuration, view definitions, and navigation shortcuts",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxocfg/",
        anchor: "`crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxocfg_configuration_examples_round_trip_fai`, `mapi_over_http_microsoft_oxocfg_writing_view_definition_sequence_succeeds`, `mapi_over_http_microsoft_oxocfg_default_named_views_expose_descriptor_columns`, `mapi_over_http_microsoft_oxocfg_navigation_shortcut_examples_round_trip`, `mapi_over_http_microsoft_oxocfg_release_persists_configuration_stream`, `crates/lpe-exchange/src/mapi/properties.rs::tests::microsoft_oxocfg_conversation_action_example_projects_fai_properties`",
        implemented: "MS-OXOCFG 4.1 dictionary, working-hours, and category-list FAI examples round-trip through associated messages and table/open-message reads. MS-OXOCFG 4.2 FolderDesign.NamedView descriptors are generated with the documented Importance, Reminder, Icon, Flag Status, Attachment, From/To, Subject, Received/Sent, Size, and Categories columns, exposed through Common Views associated-table `QueryRows`, and writable through the documented OpenStream/SetStreamSize/WriteStream/CommitStream/SaveChangesMessage sequence. MS-OXOCFG configuration streams also persist when Outlook releases the writable stream handle before saving the associated message. MS-OXOCFG 4.3.1 Conversation Action properties project the documented conversation ID, category keywords, move folder/store entry IDs, version, message class, and subject. MS-OXOCFG 4.4.1/4.4.2 Navigation Shortcut group-header and link examples round-trip through Common Views associated messages and canonical navigation shortcut storage.",
        gaps: "LPE does not claim arbitrary Outlook view designer parity beyond the bounded default mail views, persisted FAI payload preservation, and navigation shortcuts currently modeled for Outlook interoperability.",
    },
    ReportRow {
        surface: "NSPI request/property constants",
        spec: "MS-OXCMAPIHTTP NSPI request types and MS-OXPROPS address-book properties",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmapihttp/cb1f2c87-eb69-418f-9e59-c30c179615a0",
        anchor: "`crates/lpe-exchange/src/mapi/nspi.rs::nspi_request_and_property_manifests_cover_implemented_static_values`",
        implemented: "Supported NSPI request types, bootstrap property tags, and additional requested property tags are manifest-checked. Contacts project canonical MS-OXPROPS name, phone, postal-address, organization, title, department, nickname, and phonetic-name fields when LPE already stores those values. The MS-OXNSPI hierarchy-table and QueryRows examples are covered by `crates/lpe-exchange/src/tests/mapi_over_http.rs::mapi_over_http_microsoft_oxnspi_hierarchy_and_query_rows_example_round_trips`.",
        gaps: "MS-NSPI required properties not projected by LPE plus address-book office-location, phonetic-display/company, manager, rich-info, and structured home-address fields beyond stored contact address components are explicitly `KnownUnsupported`.",
    },
    ReportRow {
        surface: "EWS simple schema enums",
        spec: "EWS schema simple types",
        source: "https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange",
        anchor: "`crates/lpe-exchange/src/ews_types.rs::ews_simple_type_enums_accept_documented_mvp_values`",
        implemented: "Delete type, distinguished folder IDs used by LPE, external audience, month, OOF state, response type, task status, and weekday values are manifest-checked.",
        gaps: "Every DistinguishedFolderIdNameType value documented by Microsoft but not implemented by LPE is explicitly `KnownUnsupported`.",
    },
];

pub(crate) fn gap_status_u8(manifest: &[(u8, GapStatus)], value: u8) -> Option<GapStatus> {
    manifest
        .iter()
        .find_map(|(manifest_value, status)| (*manifest_value == value).then_some(*status))
}

pub(crate) fn gap_status_u16(manifest: &[(u16, GapStatus)], value: u16) -> Option<GapStatus> {
    manifest
        .iter()
        .find_map(|(manifest_value, status)| (*manifest_value == value).then_some(*status))
}

pub(crate) fn gap_status_u32(manifest: &[(u32, GapStatus)], value: u32) -> Option<GapStatus> {
    manifest
        .iter()
        .find_map(|(manifest_value, status)| (*manifest_value == value).then_some(*status))
}

pub(crate) fn render_gap_report() -> String {
    let mut report = String::from(
        "# Microsoft Protocol Constants Gap Report\n\n\
         <!-- Generated by crates/lpe-exchange/src/microsoft_protocol_audit.rs. Do not edit by hand. -->\n\n\
         This report tracks Microsoft-defined protocol constants that LPE currently parses,\n\
         emits, or intentionally rejects. It is scoped to constants used by LPE protocol\n\
         surfaces, not to full Microsoft Exchange feature parity.\n\n\
         Status meanings:\n\n",
    );

    for status in [
        GapStatus::Implemented,
        GapStatus::KnownUnsupported,
        GapStatus::Reserved,
        GapStatus::NotInScope,
    ] {
        let description = match status {
            GapStatus::Implemented => "LPE has a typed constant and parser/serializer coverage.",
            GapStatus::KnownUnsupported => {
                "Microsoft defines the value, and LPE names it explicitly but does not implement behavior for it."
            }
            GapStatus::Reserved => "Microsoft reserves the value.",
            GapStatus::NotInScope => {
                "the value is recognized as part of the protocol family, but it is outside LPE's current bounded interoperability surface."
            }
        };
        report.push_str(&format!("- `{}`: {description}\n", status.as_str()));
    }

    report.push_str(
        "\n| Surface | Microsoft spec | Microsoft Learn source | LPE manifest/test anchor | Implemented coverage | Explicit gaps |\n\
         | --- | --- | --- | --- | --- | --- |\n",
    );
    for row in REPORT_ROWS {
        report.push_str(&format!(
            "| {} | {} | [{}]({}) | {} | {} | {} |\n",
            row.surface, row.spec, row.spec, row.source, row.anchor, row.implemented, row.gaps
        ));
    }

    report.push_str(
        "\nCurrent audit boundary:\n\n\
         - The manifests fail tests when LPE adds a decoded/implemented constant without\n\
           also classifying it.\n\
         - The report does not imply full Exchange protocol parity.\n\
         - Behavior is intentionally unchanged by the manifests.\n",
    );
    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn microsoft_protocol_gap_report_is_current() {
        let expected = render_gap_report();
        let actual = include_str!("../../../docs/architecture/microsoft-protocol-constants-gap.md");

        assert_eq!(actual, expected);
    }
}
