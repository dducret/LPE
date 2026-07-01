use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn get_user_configuration(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let key = match parse_ews_user_configuration_key(request) {
            Ok(key) => key,
            Err(error) => {
                return Ok(operation_error_response(
                    "GetUserConfiguration",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        match self
            .store
            .fetch_ews_user_configuration(principal.account_id, &key)
            .await?
        {
            Some(configuration) => Ok(get_user_configuration_response(&configuration, request)),
            None => Ok(operation_error_response(
                "GetUserConfiguration",
                "ErrorItemNotFound",
                "User configuration was not found.",
            )),
        }
    }

    pub(in crate::service) async fn create_user_configuration(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let input = match parse_ews_user_configuration_upsert(principal, request) {
            Ok(input) => input,
            Err(error) => {
                return Ok(operation_error_response(
                    "CreateUserConfiguration",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        match self
            .store
            .upsert_ews_user_configuration(
                input,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-create-user-configuration".to_string(),
                    subject: "account_client_configurations".to_string(),
                },
            )
            .await
        {
            Ok(_) => Ok(simple_operation_success_response("CreateUserConfiguration")),
            Err(error) => Ok(operation_error_response(
                "CreateUserConfiguration",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )),
        }
    }

    pub(in crate::service) async fn update_user_configuration(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let input = match parse_ews_user_configuration_upsert(principal, request) {
            Ok(input) => input,
            Err(error) => {
                return Ok(operation_error_response(
                    "UpdateUserConfiguration",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        match self
            .store
            .upsert_ews_user_configuration(
                input,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-update-user-configuration".to_string(),
                    subject: "account_client_configurations".to_string(),
                },
            )
            .await
        {
            Ok(_) => Ok(simple_operation_success_response("UpdateUserConfiguration")),
            Err(error) => Ok(operation_error_response(
                "UpdateUserConfiguration",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )),
        }
    }

    pub(in crate::service) async fn delete_user_configuration(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let key = match parse_ews_user_configuration_key(request) {
            Ok(key) => key,
            Err(error) => {
                return Ok(operation_error_response(
                    "DeleteUserConfiguration",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                ))
            }
        };
        match self
            .store
            .delete_ews_user_configuration(
                principal.account_id,
                &key,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-delete-user-configuration".to_string(),
                    subject: "account_client_configurations".to_string(),
                },
            )
            .await
        {
            Ok(true) => Ok(simple_operation_success_response("DeleteUserConfiguration")),
            Ok(false) => Ok(operation_error_response(
                "DeleteUserConfiguration",
                "ErrorItemNotFound",
                "User configuration was not found.",
            )),
            Err(error) => Ok(operation_error_response(
                "DeleteUserConfiguration",
                ews_error_code_or(&error, "ErrorInvalidOperation"),
                &error.to_string(),
            )),
        }
    }
}

pub(in crate::service) fn get_user_configuration_response(
    configuration: &EwsUserConfiguration,
    request: &str,
) -> String {
    let properties = requested_user_configuration_properties(request);
    let dictionary = if properties.dictionary {
        ews_user_configuration_dictionary_xml(&configuration.dictionary_json)
    } else {
        String::new()
    };
    let xml_data = if properties.xml_data {
        configuration
            .xml_payload
            .as_ref()
            .map(|value| format!("<t:XmlData>{}</t:XmlData>", escape_xml(value)))
            .unwrap_or_default()
    } else {
        String::new()
    };
    let binary_data = if properties.binary_data {
        configuration
            .binary_payload
            .as_ref()
            .map(|value| {
                format!(
                    "<t:BinaryData>{}</t:BinaryData>",
                    BASE64_STANDARD.encode(value)
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    format!(
        concat!(
            "<m:GetUserConfigurationResponse>",
            "<m:ResponseMessages>",
            "<m:GetUserConfigurationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:UserConfiguration>",
            "<t:UserConfigurationName Name=\"{name}\"/>",
            "<t:ItemId Id=\"user-configuration:{id}\" ChangeKey=\"{change_key}\"/>",
            "{dictionary}",
            "{xml_data}",
            "{binary_data}",
            "</m:UserConfiguration>",
            "</m:GetUserConfigurationResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetUserConfigurationResponse>"
        ),
        name = escape_xml(&configuration.config_name),
        id = configuration.id,
        change_key = configuration.modseq,
        dictionary = dictionary,
        xml_data = xml_data,
        binary_data = binary_data,
    )
}

#[derive(Debug, Clone, Copy)]
struct RequestedUserConfigurationProperties {
    dictionary: bool,
    xml_data: bool,
    binary_data: bool,
}

fn requested_user_configuration_properties(request: &str) -> RequestedUserConfigurationProperties {
    let values = element_contents(request, "UserConfigurationProperties")
        .into_iter()
        .map(xml_text)
        .collect::<Vec<_>>();
    if values.is_empty() || values.iter().any(|value| value.eq_ignore_ascii_case("All")) {
        return RequestedUserConfigurationProperties {
            dictionary: true,
            xml_data: true,
            binary_data: true,
        };
    }
    RequestedUserConfigurationProperties {
        dictionary: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("Dictionary")),
        xml_data: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("XmlData")),
        binary_data: values
            .iter()
            .any(|value| value.eq_ignore_ascii_case("BinaryData")),
    }
}

fn ews_user_configuration_dictionary_xml(dictionary: &serde_json::Value) -> String {
    let Some(object) = dictionary.as_object() else {
        return "<t:Dictionary/>".to_string();
    };
    if object.is_empty() {
        return "<t:Dictionary/>".to_string();
    }
    let entries = object
        .iter()
        .map(|(key, value)| {
            let value = value.as_str().unwrap_or_default();
            format!(
                concat!(
                    "<t:DictionaryEntry>",
                    "<t:DictionaryKey><t:Type>String</t:Type><t:Value>{key}</t:Value></t:DictionaryKey>",
                    "<t:DictionaryValue><t:Type>String</t:Type><t:Value>{value}</t:Value></t:DictionaryValue>",
                    "</t:DictionaryEntry>"
                ),
                key = escape_xml(key),
                value = escape_xml(value),
            )
        })
        .collect::<String>();
    format!("<t:Dictionary>{entries}</t:Dictionary>")
}

pub(in crate::service) fn parse_ews_user_configuration_key(
    request: &str,
) -> Result<EwsUserConfigurationKey> {
    let name_element = element_content(request, "UserConfigurationName")
        .ok_or_else(|| anyhow!("UserConfigurationName is required."))?;
    let open_tag = open_tag_text(request, "UserConfigurationName")
        .ok_or_else(|| anyhow!("UserConfigurationName is required."))?;
    let config_name = attribute_value(open_tag, "Name")
        .map(xml_text)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("UserConfigurationName Name is required."))?;
    let folder_id = attribute_value_after(name_element, "FolderId", "Id")
        .or_else(|| attribute_value_after(name_element, "DistinguishedFolderId", "Id"))
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let (scope_kind, mailbox_id, public_folder_id) = if let Some(folder_id) = folder_id {
        if let Some(raw_id) = folder_id.strip_prefix("mailbox:") {
            (
                "mailbox".to_string(),
                Some(Uuid::parse_str(raw_id).map_err(|_| anyhow!("invalid mailbox FolderId"))?),
                None,
            )
        } else if let Some(raw_id) = folder_id.strip_prefix("public-folder:") {
            (
                "public_folder".to_string(),
                None,
                Some(
                    Uuid::parse_str(raw_id)
                        .map_err(|_| anyhow!("invalid public folder FolderId"))?,
                ),
            )
        } else {
            bail!(
                "UserConfigurationName FolderId must be mailbox:{{uuid}} or public-folder:{{uuid}}."
            );
        }
    } else {
        ("account".to_string(), None, None)
    };
    Ok(EwsUserConfigurationKey {
        scope_kind,
        mailbox_id,
        public_folder_id,
        config_name,
        config_class: "ews_user_configuration".to_string(),
    })
}

pub(in crate::service) fn parse_ews_user_configuration_upsert(
    principal: &AccountPrincipal,
    request: &str,
) -> Result<UpsertEwsUserConfigurationInput> {
    let key = parse_ews_user_configuration_key(request)?;
    let dictionary_json = parse_ews_user_configuration_dictionary(request)?;
    let xml_payload = element_text(request, "XmlData").filter(|value| !value.is_empty());
    let binary_payload = element_text(request, "BinaryData")
        .filter(|value| !value.is_empty())
        .map(|value| {
            BASE64_STANDARD
                .decode(value.as_bytes())
                .map_err(|_| anyhow!("BinaryData must be valid base64."))
        })
        .transpose()?;
    Ok(UpsertEwsUserConfigurationInput {
        account_id: principal.account_id,
        key,
        dictionary_json,
        xml_payload,
        binary_payload,
    })
}

fn parse_ews_user_configuration_dictionary(request: &str) -> Result<serde_json::Value> {
    let Some(dictionary) = element_content(request, "Dictionary") else {
        return Ok(serde_json::json!({}));
    };
    let mut object = serde_json::Map::new();
    for entry in element_contents(dictionary, "DictionaryEntry") {
        let key = element_content(entry, "DictionaryKey")
            .and_then(|content| element_text(content, "Value"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("DictionaryEntry requires DictionaryKey Value."))?;
        let value = element_content(entry, "DictionaryValue")
            .and_then(|content| element_text(content, "Value"))
            .unwrap_or_default();
        object.insert(key, serde_json::Value::String(value));
    }
    Ok(serde_json::Value::Object(object))
}
