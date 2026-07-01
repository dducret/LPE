#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) enum TypedRopRequest {
    Release(RopInputOnlyRequest),
    OpenFolder(RopOpenFolderRequest),
    OpenMessage(RopOpenMessageRequest),
    OpenTable(RopOpenTableRequest),
    CreateMessage(RopCreateMessageRequest),
    SaveChangesMessage(RopSaveChangesMessageRequest),
    OpenEmbeddedMessage(RopOpenEmbeddedMessageRequest),
    SetColumns(RopSetColumnsRequest),
    Restrict(RopRestrictionRequest),
    QueryRows(RopQueryRowsRequest),
    Logon(RopLogonRequest),
    SupportedRaw(RopSupportedRawRequest),
    Unsupported(RopUnsupportedRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopInputOnlyRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenFolderRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) open_mode_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) open_mode_flags: u8,
    pub(in crate::mapi) message_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenTableRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) table_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopCreateMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) folder_id: u64,
    pub(in crate::mapi) associated_flag: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSaveChangesMessageRequest {
    pub(in crate::mapi) response_handle_index: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) save_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopOpenEmbeddedMessageRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) code_page_id: u16,
    pub(in crate::mapi) open_mode_flags: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSetColumnsRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) property_tags: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopRestrictionRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) restriction: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopQueryRowsRequest {
    pub(in crate::mapi) input_handle_index: u8,
    pub(in crate::mapi) flags: u8,
    pub(in crate::mapi) forward_read: bool,
    pub(in crate::mapi) row_count: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopLogonRequest {
    pub(in crate::mapi) output_handle_index: u8,
    pub(in crate::mapi) logon_flags: u8,
    pub(in crate::mapi) prefix: Vec<u8>,
    pub(in crate::mapi) essdn: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopSupportedRawRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) output_handle_index: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::mapi) struct RopUnsupportedRequest {
    pub(in crate::mapi) rop_id: u8,
    pub(in crate::mapi) input_handle_index: Option<u8>,
    pub(in crate::mapi) reserved: bool,
}
