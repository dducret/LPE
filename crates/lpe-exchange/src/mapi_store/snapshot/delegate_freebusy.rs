use super::*;

impl MapiMailStoreSnapshot {
    pub(crate) fn with_delegate_freebusy_messages(
        mut self,
        messages: Vec<DelegateFreeBusyMessageObject>,
    ) -> Self {
        self.delegate_freebusy_messages = messages
            .into_iter()
            .map(|message| MapiDelegateFreeBusyMessage {
                id: mapi_item_id(&message.id),
                folder_id: crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID,
                canonical_id: message.id,
                durable_identity: None,
                delegates: Vec::new(),
                custom_properties: Vec::new(),
                message,
            })
            .collect();
        self
    }

    pub(crate) fn with_delegate_freebusy_message_identities(
        mut self,
        identity_records: &[MapiIdentityRecord],
    ) -> Result<Self> {
        let identities = identity_records
            .iter()
            .filter(|identity| {
                identity.object_kind == MapiIdentityObjectKind::DelegateFreeBusyMessage
            })
            .map(|identity| (identity.canonical_id, identity))
            .collect::<HashMap<_, _>>();
        for message in &mut self.delegate_freebusy_messages {
            if let Some(identity) = identities.get(&message.canonical_id) {
                message.id = identity.object_id;
                message.durable_identity = Some((*identity).clone());
            }
        }
        let local_freebusy_identity = identities
            .get(&OUTLOOK_LOCAL_FREEBUSY_CANONICAL_ID)
            .ok_or_else(|| anyhow!("durable MAPI LocalFreebusy identity is missing"))?;
        ensure_virtual_local_freebusy_message(
            &mut self.delegate_freebusy_messages,
            local_freebusy_identity,
        );
        Ok(self)
    }

    pub(crate) fn with_local_freebusy_delegates(
        mut self,
        delegates: Vec<EwsDelegate>,
    ) -> Result<Self> {
        let message = self
            .delegate_freebusy_messages
            .iter_mut()
            .find(|message| is_outlook_local_freebusy_message(message))
            .ok_or_else(|| anyhow!("canonical MAPI LocalFreebusy message is missing"))?;
        message.delegates = delegates;
        Ok(self)
    }

    pub(crate) fn with_local_freebusy_custom_properties(
        mut self,
        custom_properties: Vec<MapiCustomPropertyValue>,
    ) -> Result<Self> {
        let message = self
            .delegate_freebusy_messages
            .iter_mut()
            .find(|message| is_outlook_local_freebusy_message(message))
            .ok_or_else(|| anyhow!("canonical MAPI LocalFreebusy message is missing"))?;
        message.custom_properties = custom_properties;
        Ok(self)
    }

    pub(crate) fn delegate_freebusy_messages(&self) -> &[MapiDelegateFreeBusyMessage] {
        &self.delegate_freebusy_messages
    }

    pub(crate) fn delegate_freebusy_message_for_id(
        &self,
        item_id: u64,
    ) -> Option<&MapiDelegateFreeBusyMessage> {
        self.delegate_freebusy_messages
            .iter()
            .find(|message| message.id == item_id)
    }

    pub(crate) fn local_freebusy_message_id(&self) -> Option<u64> {
        self.delegate_freebusy_messages
            .iter()
            .find(|message| is_outlook_local_freebusy_message(message))
            .map(|message| message.id)
    }

    pub(crate) fn is_outlook_local_freebusy_message_id(&self, item_id: u64) -> bool {
        self.local_freebusy_message_id() == Some(item_id)
    }
}
