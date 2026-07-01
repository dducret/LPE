use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_im_item_list(
        &self,
        principal: &AccountPrincipal,
    ) -> Result<String> {
        let list = self.store.fetch_ews_im_list(principal).await?;
        Ok(get_im_item_list_response(&list))
    }

    pub(in crate::service) async fn get_im_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let list = self.store.fetch_ews_im_list(principal).await?;
        Ok(get_im_items_response(request, &list))
    }

    pub(in crate::service) async fn add_im_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let display_name = requested_im_group_name(request)
            .ok_or_else(|| anyhow!("AddImGroup requires a group display name"))?;
        let group = self
            .store
            .upsert_ews_im_group(
                principal,
                None,
                &display_name,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-add-im-group".to_string(),
                    subject: display_name.clone(),
                },
            )
            .await?;
        Ok(im_group_operation_response("AddImGroup", &group))
    }

    pub(in crate::service) async fn set_im_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("SetImGroup requires a group id"))?;
        let display_name = requested_im_group_name(request)
            .ok_or_else(|| anyhow!("SetImGroup requires a group display name"))?;
        let group = self
            .store
            .upsert_ews_im_group(
                principal,
                Some(group_id),
                &display_name,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-set-im-group".to_string(),
                    subject: group_id.to_string(),
                },
            )
            .await?;
        Ok(im_group_operation_response("SetImGroup", &group))
    }

    pub(in crate::service) async fn remove_im_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("RemoveImGroup requires a group id"))?;
        let removed = self
            .store
            .remove_ews_im_group(
                principal,
                group_id,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-remove-im-group".to_string(),
                    subject: group_id.to_string(),
                },
            )
            .await?;
        Ok(simple_ews_operation_result("RemoveImGroup", removed))
    }

    pub(in crate::service) async fn add_im_contact_to_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("AddImContactToGroup requires a group id"))?;
        let member = requested_im_contact_member(request, principal).ok_or_else(|| {
            anyhow!("AddImContactToGroup requires a visible contact or account member")
        })?;
        let member = self
            .store
            .add_ews_im_group_member(
                principal,
                group_id,
                member,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-add-im-contact-to-group".to_string(),
                    subject: group_id.to_string(),
                },
            )
            .await?;
        Ok(im_member_operation_response("AddImContactToGroup", &member))
    }

    pub(in crate::service) async fn add_new_im_contact_to_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("AddNewImContactToGroup requires a group id"))?;
        let name = element_text(request, "DisplayName")
            .or_else(|| element_text(request, "Name"))
            .unwrap_or_else(|| "IM Contact".to_string());
        let email = requested_smtp_address(request)
            .ok_or_else(|| anyhow!("AddNewImContactToGroup requires an SMTP address"))?;
        let contact = self
            .store
            .create_accessible_contact(
                principal.account_id,
                Some("im_contact_list"),
                UpsertClientContactInput {
                    account_id: principal.account_id,
                    id: None,
                    name,
                    role: String::new(),
                    email,
                    phone: String::new(),
                    team: String::new(),
                    notes: String::new(),
                    ..Default::default()
                },
            )
            .await?;
        let member = self
            .store
            .add_ews_im_group_member(
                principal,
                group_id,
                EwsImMemberInput {
                    member_kind: "contact".to_string(),
                    contact_id: Some(contact.id),
                    account_id: None,
                    external_address: None,
                    display_name: contact.name.clone(),
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-add-new-im-contact-to-group".to_string(),
                    subject: group_id.to_string(),
                },
            )
            .await?;
        Ok(im_member_operation_response(
            "AddNewImContactToGroup",
            &member,
        ))
    }

    pub(in crate::service) async fn add_new_tel_uri_contact_to_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("AddNewTelUriContactToGroup requires a group id"))?;
        let tel_uri = element_text(request, "TelUri")
            .or_else(|| element_text(request, "Address"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("AddNewTelUriContactToGroup requires a tel URI"))?;
        let display_name = element_text(request, "DisplayName")
            .or_else(|| element_text(request, "Name"))
            .unwrap_or_else(|| tel_uri.clone());
        let member = self
            .store
            .add_ews_im_group_member(
                principal,
                group_id,
                EwsImMemberInput {
                    member_kind: "tel_uri".to_string(),
                    contact_id: None,
                    account_id: None,
                    external_address: Some(tel_uri),
                    display_name,
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-add-new-tel-uri-contact-to-group".to_string(),
                    subject: group_id.to_string(),
                },
            )
            .await?;
        Ok(im_member_operation_response(
            "AddNewTelUriContactToGroup",
            &member,
        ))
    }

    pub(in crate::service) async fn remove_contact_from_im_list(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let value = requested_im_member_value(request)
            .ok_or_else(|| anyhow!("RemoveContactFromImList requires a member id"))?;
        let removed = self
            .store
            .remove_ews_im_group_member(
                principal,
                None,
                requested_im_member_kind(request).unwrap_or("contact"),
                &value,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-remove-contact-from-im-list".to_string(),
                    subject: value.clone(),
                },
            )
            .await?;
        Ok(simple_ews_operation_result(
            "RemoveContactFromImList",
            removed,
        ))
    }

    pub(in crate::service) async fn remove_im_contact_from_group(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("RemoveImContactFromGroup requires a group id"))?;
        let value = requested_im_member_value(request)
            .ok_or_else(|| anyhow!("RemoveImContactFromGroup requires a member id"))?;
        let removed = self
            .store
            .remove_ews_im_group_member(
                principal,
                Some(group_id),
                requested_im_member_kind(request).unwrap_or("contact"),
                &value,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-remove-im-contact-from-group".to_string(),
                    subject: format!("{group_id}:{value}"),
                },
            )
            .await?;
        Ok(simple_ews_operation_result(
            "RemoveImContactFromGroup",
            removed,
        ))
    }

    pub(in crate::service) async fn add_distribution_group_to_im_list(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let group_id = requested_im_group_id(request)
            .ok_or_else(|| anyhow!("AddDistributionGroupToImList requires a group id"))?;
        let smtp = requested_smtp_address(request).ok_or_else(|| {
            anyhow!("AddDistributionGroupToImList requires a distribution-list SMTP address")
        })?;
        let entries = self.store.fetch_address_book_entries(principal).await?;
        let entry = entries
            .iter()
            .find(|entry| {
                entry.entry_kind == ExchangeAddressBookEntryKind::DistributionList
                    && entry.email.eq_ignore_ascii_case(&smtp)
            })
            .ok_or_else(|| anyhow!("distribution list not found"))?;
        let member = self
            .store
            .add_ews_im_group_member(
                principal,
                group_id,
                EwsImMemberInput {
                    member_kind: "distribution_group".to_string(),
                    contact_id: None,
                    account_id: None,
                    external_address: Some(entry.email.clone()),
                    display_name: entry.display_name.clone(),
                },
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-add-distribution-group-to-im-list".to_string(),
                    subject: entry.email.clone(),
                },
            )
            .await?;
        Ok(im_member_operation_response(
            "AddDistributionGroupToImList",
            &member,
        ))
    }

    pub(in crate::service) async fn remove_distribution_group_from_im_list(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let smtp = requested_smtp_address(request).ok_or_else(|| {
            anyhow!("RemoveDistributionGroupFromImList requires a distribution-list SMTP address")
        })?;
        let removed = self
            .store
            .remove_ews_im_group_member(
                principal,
                requested_im_group_id(request),
                "distribution_group",
                &smtp,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-remove-distribution-group-from-im-list".to_string(),
                    subject: smtp.clone(),
                },
            )
            .await?;
        Ok(simple_ews_operation_result(
            "RemoveDistributionGroupFromImList",
            removed,
        ))
    }
}

pub(in crate::service) fn get_im_item_list_response(list: &EwsImList) -> String {
    let groups_xml = list.groups.iter().map(im_group_xml).collect::<String>();
    let members_xml = list.members.iter().map(im_member_xml).collect::<String>();
    format!(
        concat!(
            "<m:GetImItemListResponse>",
            "<m:ResponseMessages>",
            "<m:GetImItemListResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ImGroups>{groups_xml}</m:ImGroups>",
            "<m:ImItems>{members_xml}</m:ImItems>",
            "</m:GetImItemListResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetImItemListResponse>"
        ),
        groups_xml = groups_xml,
        members_xml = members_xml,
    )
}

pub(in crate::service) fn get_im_items_response(request: &str, list: &EwsImList) -> String {
    let requested_ids = attribute_values_for_tag(request, "ImItemId", "Id")
        .into_iter()
        .chain(attribute_values_for_tag(request, "ItemId", "Id"))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let mut members = list.members.clone();
    if !requested_ids.is_empty() {
        members.retain(|member| requested_ids.contains(&im_member_id(member)));
    }
    let members_xml = members.iter().map(im_member_xml).collect::<String>();
    format!(
        concat!(
            "<m:GetImItemsResponse>",
            "<m:ResponseMessages>",
            "<m:GetImItemsResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:ImItems>{members_xml}</m:ImItems>",
            "</m:GetImItemsResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetImItemsResponse>"
        ),
        members_xml = members_xml,
    )
}

pub(in crate::service) fn im_group_operation_response(
    operation: &str,
    group: &EwsImGroup,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{group_xml}",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        group_xml = im_group_xml(group),
    )
}

pub(in crate::service) fn im_member_operation_response(
    operation: &str,
    member: &EwsImGroupMember,
) -> String {
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "{member_xml}",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        member_xml = im_member_xml(member),
    )
}

pub(in crate::service) fn simple_ews_operation_result(operation: &str, ok: bool) -> String {
    if ok {
        simple_operation_success_response(operation)
    } else {
        operation_error_response(operation, "ErrorItemNotFound", "UCS item not found")
    }
}

pub(in crate::service) fn requested_smtp_address(request: &str) -> Option<String> {
    element_text(request, "SmtpAddress")
        .or_else(|| element_text(request, "EmailAddress"))
        .or_else(|| {
            element_content(request, "Mailbox")
                .and_then(|mailbox| element_text(mailbox, "EmailAddress"))
        })
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

pub(in crate::service) fn requested_im_group_id(request: &str) -> Option<Uuid> {
    attribute_values_for_tag(request, "ImGroupId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "GroupId", "Id")
                .into_iter()
                .next()
        })
        .or_else(|| {
            attribute_values_for_tag(request, "ItemId", "Id")
                .into_iter()
                .next()
        })
        .map(str::to_string)
        .or_else(|| element_text(request, "ImGroupId"))
        .or_else(|| element_text(request, "GroupId"))
        .and_then(|value| parse_prefixed_uuid(&value, "im-group:"))
}

pub(in crate::service) fn requested_im_group_name(request: &str) -> Option<String> {
    element_text(request, "DisplayName")
        .or_else(|| element_text(request, "GroupName"))
        .or_else(|| element_text(request, "Name"))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(in crate::service) fn requested_im_member_kind(request: &str) -> Option<&'static str> {
    element_text(request, "MemberKind")
        .or_else(|| element_text(request, "ImAddressType"))
        .map(|value| value.to_ascii_lowercase())
        .and_then(|value| match value.as_str() {
            "contact" | "imcontact" => Some("contact"),
            "account" | "mailbox" => Some("account"),
            "distribution_group" | "distributiongroup" | "publicdl" => Some("distribution_group"),
            "tel_uri" | "teluri" | "telephone" => Some("tel_uri"),
            _ => None,
        })
        .or_else(|| {
            if element_text(request, "AccountId").is_some() {
                Some("account")
            } else if element_text(request, "TelUri").is_some() {
                Some("tel_uri")
            } else {
                None
            }
        })
}

pub(in crate::service) fn requested_im_member_value(request: &str) -> Option<String> {
    attribute_values_for_tag(request, "ImContactId", "Id")
        .into_iter()
        .next()
        .or_else(|| {
            attribute_values_for_tag(request, "ContactId", "Id")
                .into_iter()
                .next()
        })
        .or_else(|| {
            attribute_values_for_tag(request, "MemberId", "Id")
                .into_iter()
                .next()
        })
        .map(str::to_string)
        .or_else(|| element_text(request, "ContactId"))
        .or_else(|| element_text(request, "AccountId"))
        .or_else(|| element_text(request, "MemberId"))
        .or_else(|| element_text(request, "TelUri"))
        .or_else(|| requested_smtp_address(request))
        .map(|value| {
            value
                .strip_prefix("im-member:")
                .and_then(|rest| rest.split_once(':').map(|(_, value)| value.to_string()))
                .or_else(|| value.strip_prefix("contact:").map(str::to_string))
                .or_else(|| value.strip_prefix("account:").map(str::to_string))
                .unwrap_or(value)
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

pub(in crate::service) fn requested_im_contact_member(
    request: &str,
    principal: &AccountPrincipal,
) -> Option<EwsImMemberInput> {
    if let Some(account_id) =
        element_text(request, "AccountId").and_then(|value| parse_prefixed_uuid(&value, "account:"))
    {
        return Some(EwsImMemberInput {
            member_kind: "account".to_string(),
            contact_id: None,
            account_id: Some(account_id),
            external_address: None,
            display_name: element_text(request, "DisplayName")
                .unwrap_or_else(|| account_id.to_string()),
        });
    }
    let value = requested_im_member_value(request)?;
    let id = parse_prefixed_uuid(&value, "contact:")?;
    Some(EwsImMemberInput {
        member_kind: "contact".to_string(),
        contact_id: Some(id),
        account_id: None,
        external_address: None,
        display_name: element_text(request, "DisplayName").unwrap_or_else(|| {
            if id == principal.account_id {
                principal.display_name.clone()
            } else {
                id.to_string()
            }
        }),
    })
}

fn im_group_xml(group: &EwsImGroup) -> String {
    format!(
        concat!(
            "<t:ImGroup>",
            "<t:ImGroupId Id=\"im-group:{id}\" ChangeKey=\"{modseq}\"/>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "</t:ImGroup>"
        ),
        id = group.id,
        modseq = group.modseq,
        display_name = escape_xml(&group.display_name),
    )
}

fn im_member_xml(member: &EwsImGroupMember) -> String {
    let value = im_member_value(member);
    format!(
        concat!(
            "<t:ImItem>",
            "<t:ImItemId Id=\"{member_id}\"/>",
            "<t:ParentGroupId Id=\"im-group:{group_id}\"/>",
            "<t:MemberKind>{kind}</t:MemberKind>",
            "<t:DisplayName>{display_name}</t:DisplayName>",
            "<t:SmtpAddress>{value}</t:SmtpAddress>",
            "</t:ImItem>"
        ),
        member_id = escape_xml(&im_member_id(member)),
        group_id = member.group_id,
        kind = escape_xml(&member.member_kind),
        display_name = escape_xml(&member.display_name),
        value = escape_xml(&value),
    )
}

fn im_member_id(member: &EwsImGroupMember) -> String {
    format!(
        "im-member:{}:{}",
        member.member_kind,
        im_member_value(member)
    )
}

fn im_member_value(member: &EwsImGroupMember) -> String {
    member
        .contact_id
        .or(member.account_id)
        .map(|id| id.to_string())
        .or_else(|| member.external_address.clone())
        .unwrap_or_default()
}

fn parse_prefixed_uuid(value: &str, prefix: &str) -> Option<Uuid> {
    Uuid::parse_str(value.strip_prefix(prefix).unwrap_or(value)).ok()
}
