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
    (0x34, GapStatus::KnownUnsupported),
    (0x35, GapStatus::Implemented),
    (0x36, GapStatus::Implemented),
    (0x37, GapStatus::Implemented),
    (0x38, GapStatus::Implemented),
    (0x39, GapStatus::KnownUnsupported),
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
    (0x46, GapStatus::KnownUnsupported),
    (0x47, GapStatus::KnownUnsupported),
    (0x48, GapStatus::KnownUnsupported),
    (0x49, GapStatus::Implemented),
    (0x4A, GapStatus::Implemented),
    (0x4B, GapStatus::Implemented),
    (0x4C, GapStatus::Implemented),
    (0x4D, GapStatus::Implemented),
    (0x4E, GapStatus::Implemented),
    (0x4F, GapStatus::Implemented),
    (0x50, GapStatus::Implemented),
    (0x51, GapStatus::KnownUnsupported),
    (0x52, GapStatus::Implemented),
    (0x53, GapStatus::Implemented),
    (0x54, GapStatus::Implemented),
    (0x55, GapStatus::Implemented),
    (0x56, GapStatus::Implemented),
    (0x57, GapStatus::Implemented),
    (0x58, GapStatus::Implemented),
    (0x59, GapStatus::KnownUnsupported),
    (0x5A, GapStatus::KnownUnsupported),
    (0x5B, GapStatus::KnownUnsupported),
    (0x5C, GapStatus::KnownUnsupported),
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
    (0x86, GapStatus::KnownUnsupported),
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
    (0x0004, GapStatus::KnownUnsupported),
    (0x0005, GapStatus::KnownUnsupported),
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
    (0x00FB, GapStatus::KnownUnsupported),
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
    (0x1048, GapStatus::Implemented),
    (0x10FB, GapStatus::KnownUnsupported),
    (0x10FD, GapStatus::KnownUnsupported),
    (0x10FE, GapStatus::KnownUnsupported),
    (0x1102, GapStatus::Implemented),
];

pub(crate) const FAST_TRANSFER_MARKER_GAP_MANIFEST: &[(u32, GapStatus)] = &[
    (0x4000_0003, GapStatus::KnownUnsupported),
    (0x4001_0003, GapStatus::KnownUnsupported),
    (0x4002_0003, GapStatus::KnownUnsupported),
    (0x4003_0003, GapStatus::KnownUnsupported),
    (0x4004_0003, GapStatus::KnownUnsupported),
    (0x4009_0003, GapStatus::KnownUnsupported),
    (0x400A_0003, GapStatus::KnownUnsupported),
    (0x400B_0003, GapStatus::KnownUnsupported),
    (0x400C_0003, GapStatus::KnownUnsupported),
    (0x400D_0003, GapStatus::KnownUnsupported),
    (0x400E_0003, GapStatus::KnownUnsupported),
    (0x4010_0003, GapStatus::KnownUnsupported),
    (0x4012_0003, GapStatus::Implemented),
    (0x4013_0003, GapStatus::Implemented),
    (0x4014_0003, GapStatus::Implemented),
    (0x4015_0003, GapStatus::Implemented),
    (0x4018_0003, GapStatus::KnownUnsupported),
    (0x402F_0003, GapStatus::Implemented),
    (0x403A_0003, GapStatus::Implemented),
    (0x403B_0003, GapStatus::Implemented),
    (0x4074_000B, GapStatus::KnownUnsupported),
    (0x4075_000B, GapStatus::KnownUnsupported),
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
        implemented: "EMSMDB and NSPI request types implemented by LPE are manifest-checked.",
        gaps: "Unknown request types remain `Unsupported(String)` diagnostics.",
    },
    ReportRow {
        surface: "ROP IDs",
        spec: "MS-OXCROPS RopId table",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcrops/6c623489-576d-45ef-9288-5b62b73c6961",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::ROP_ID_GAP_MANIFEST`",
        implemented: "Every `RopId::from_u8` decoded value must be classified by test.",
        gaps: "Selected decoded values are `KnownUnsupported` or `NotInScope`, including `RopAbortSubmit`, `RopCopyTo`, `RopOpenEmbeddedMessage`, `RopSetSpooler`, `RopSpoolerLockMessage`, `RopTransportNewMail`, row expand/collapse, stream region lock/unlock, `RopTellVersion`, notify/pending, and reserved entries.",
    },
    ReportRow {
        surface: "MAPI property types",
        spec: "MS-OXCDATA property data types",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcdata/0c77892e-288e-435a-9c49-be1c20c7afdb",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::PROPERTY_TYPE_GAP_MANIFEST`",
        implemented: "Implemented scalar and multivalue property types are manifest-checked.",
        gaps: "`PtypUnspecified`, `PtypNull`, floating/currency/object/serverId/restriction/ruleAction and related multivalue forms are explicitly `KnownUnsupported`.",
    },
    ReportRow {
        surface: "FastTransfer/ICS markers",
        spec: "MS-OXCFXICS markers",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcfxics/e8e45630-33dd-4974-84d0-7b68a037a724",
        anchor: "`crates/lpe-exchange/src/microsoft_protocol_audit.rs::FAST_TRANSFER_MARKER_GAP_MANIFEST`",
        implemented: "Incremental sync markers used by LPE are manifest-checked.",
        gaps: "All other MS-OXCFXICS markers in the documented marker table are explicitly `KnownUnsupported`, including folder/message/embed/recipient/attachment markers, progress/group markers, partial change markers, and `FXErrorInfo`.",
    },
    ReportRow {
        surface: "NSPI request/property constants",
        spec: "MS-OXCMAPIHTTP NSPI request types and MS-OXPROPS address-book properties",
        source: "https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmapihttp/cb1f2c87-eb69-418f-9e59-c30c179615a0",
        anchor: "`crates/lpe-exchange/src/mapi/nspi.rs::nspi_request_and_property_manifests_cover_implemented_static_values`",
        implemented: "Supported NSPI request types, bootstrap property tags, and additional requested property tags are manifest-checked.",
        gaps: "MS-NSPI required properties not projected by LPE plus common MS-OXPROPS address-book name, organization, phone, postal, phonetic, and manager fields are explicitly `KnownUnsupported`.",
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
