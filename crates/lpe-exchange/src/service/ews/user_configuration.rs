use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

use super::super::*;

const MAX_USER_CONFIGURATION_NAME_BYTES: usize = 256;
const MAX_USER_CONFIGURATION_DICTIONARY_ENTRIES: usize = 128;
const MAX_USER_CONFIGURATION_DICTIONARY_KEY_BYTES: usize = 256;
const MAX_USER_CONFIGURATION_DICTIONARY_VALUE_BYTES: usize = 4096;
const MAX_USER_CONFIGURATION_PAYLOAD_BYTES: usize = 64 * 1024;

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
        if let Err(error) = self
            .validate_ews_user_configuration_scope(principal, &key)
            .await
        {
            return Ok(operation_error_response(
                "GetUserConfiguration",
                "ErrorAccessDenied",
                &error.to_string(),
            ));
        }
        match self
            .store
            .fetch_ews_user_configuration(principal.account_id, &key)
            .await?
        {
            Some(configuration) => match get_user_configuration_response(&configuration, request) {
                Ok(response) => Ok(response),
                Err(error) => Ok(operation_error_response(
                    "GetUserConfiguration",
                    "ErrorInvalidOperation",
                    &error.to_string(),
                )),
            },
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
        if let Err(error) = self
            .validate_ews_user_configuration_scope(principal, &input.key)
            .await
        {
            return Ok(operation_error_response(
                "CreateUserConfiguration",
                "ErrorAccessDenied",
                &error.to_string(),
            ));
        }
        match self
            .store
            .create_ews_user_configuration(
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
        if let Err(error) = self
            .validate_ews_user_configuration_scope(principal, &input.key)
            .await
        {
            return Ok(operation_error_response(
                "UpdateUserConfiguration",
                "ErrorAccessDenied",
                &error.to_string(),
            ));
        }
        match self
            .store
            .update_ews_user_configuration(
                input,
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-update-user-configuration".to_string(),
                    subject: "account_client_configurations".to_string(),
                },
            )
            .await
        {
            Ok(Some(_)) => Ok(simple_operation_success_response("UpdateUserConfiguration")),
            Ok(None) => Ok(operation_error_response(
                "UpdateUserConfiguration",
                "ErrorItemNotFound",
                "User configuration was not found.",
            )),
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
        if let Err(error) = self
            .validate_ews_user_configuration_scope(principal, &key)
            .await
        {
            return Ok(operation_error_response(
                "DeleteUserConfiguration",
                "ErrorAccessDenied",
                &error.to_string(),
            ));
        }
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

    async fn validate_ews_user_configuration_scope(
        &self,
        principal: &AccountPrincipal,
        key: &EwsUserConfigurationKey,
    ) -> Result<()> {
        // [MS-OXWSUSRCFG] §§3.1.4.1-.4: validate a scoped configuration target
        // before reading or mutating the canonical account configuration row.
        match key.scope_kind.as_str() {
            "account" => Ok(()),
            "mailbox" => {
                let mailbox_id = key
                    .mailbox_id
                    .ok_or_else(|| anyhow!("mailbox scope is not accessible"))?;
                if self
                    .store
                    .fetch_jmap_mailboxes(principal.account_id)
                    .await?
                    .iter()
                    .any(|mailbox| mailbox.id == mailbox_id)
                {
                    Ok(())
                } else {
                    bail!("mailbox scope is not accessible")
                }
            }
            "public_folder" => {
                let folder_id = key
                    .public_folder_id
                    .ok_or_else(|| anyhow!("public folder scope is not accessible"))?;
                let folder = self
                    .store
                    .fetch_public_folder(principal.account_id, folder_id)
                    .await
                    .map_err(|_| anyhow!("public folder scope is not accessible"))?;
                if folder.rights.may_read {
                    Ok(())
                } else {
                    bail!("public folder read access is not granted")
                }
            }
            _ => bail!("user configuration scope is not supported"),
        }
    }
}

pub(in crate::service) fn get_user_configuration_response(
    configuration: &EwsUserConfiguration,
    request: &str,
) -> Result<String> {
    let properties = requested_user_configuration_properties(request)?;
    let configuration_name = ews_user_configuration_name_xml(configuration);
    let item_id = if properties.id {
        format!(
            "<t:ItemId Id=\"user-configuration:{id}\" ChangeKey=\"{change_key}\"/>",
            id = configuration.id,
            change_key = configuration.modseq,
        )
    } else {
        String::new()
    };
    let dictionary = if properties.dictionary {
        ews_user_configuration_dictionary_xml(&configuration.dictionary_json)
    } else {
        String::new()
    };
    let xml_data = if properties.xml_data {
        configuration
            .xml_payload
            .as_ref()
            .map(|value| format!("<t:XmlData>{}</t:XmlData>", BASE64_STANDARD.encode(value)))
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
    Ok(format!(
        concat!(
            "<m:GetUserConfigurationResponse>",
            "<m:ResponseMessages>",
            "<m:GetUserConfigurationResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:UserConfiguration>",
            "{configuration_name}",
            "{item_id}",
            "{dictionary}",
            "{xml_data}",
            "{binary_data}",
            "</m:UserConfiguration>",
            "</m:GetUserConfigurationResponseMessage>",
            "</m:ResponseMessages>",
            "</m:GetUserConfigurationResponse>"
        ),
        configuration_name = configuration_name,
        item_id = item_id,
        dictionary = dictionary,
        xml_data = xml_data,
        binary_data = binary_data,
    ))
}

#[derive(Debug, Clone, Copy)]
struct RequestedUserConfigurationProperties {
    id: bool,
    dictionary: bool,
    xml_data: bool,
    binary_data: bool,
}

fn requested_user_configuration_properties(
    request: &str,
) -> Result<RequestedUserConfigurationProperties> {
    let values = element_contents(request, "UserConfigurationProperties")
        .into_iter()
        .map(xml_text)
        .collect::<Vec<_>>();
    if values.is_empty() || values.iter().any(|value| value.eq_ignore_ascii_case("All")) {
        if values.len() > 1 {
            bail!("UserConfigurationProperties cannot combine All with other properties");
        }
        return Ok(RequestedUserConfigurationProperties {
            id: true,
            dictionary: true,
            xml_data: true,
            binary_data: true,
        });
    }
    let mut unique_values = std::collections::HashSet::new();
    if values.iter().any(|value| {
        !matches!(
            value.as_str(),
            "Id" | "Dictionary" | "XmlData" | "BinaryData"
        )
    }) || values.iter().any(|value| !unique_values.insert(value))
    {
        bail!("UserConfigurationProperties contains an unsupported or duplicate property");
    }
    Ok(RequestedUserConfigurationProperties {
        id: values.iter().any(|value| value == "Id"),
        dictionary: values.iter().any(|value| value == "Dictionary"),
        xml_data: values.iter().any(|value| value == "XmlData"),
        binary_data: values.iter().any(|value| value == "BinaryData"),
    })
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
    if config_name.len() > MAX_USER_CONFIGURATION_NAME_BYTES {
        bail!("UserConfigurationName Name exceeds the supported limit.");
    }
    let folder_ids = attribute_values_for_tag(name_element, "FolderId", "Id");
    let distinguished_folder_ids =
        attribute_values_for_tag(name_element, "DistinguishedFolderId", "Id");
    if folder_ids.len() + distinguished_folder_ids.len() > 1 {
        bail!("UserConfigurationName accepts at most one folder scope.");
    }
    let folder_id = folder_ids
        .into_iter()
        .chain(distinguished_folder_ids)
        .next()
        .map(|value| value.trim().to_string())
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
    let xml_payload = element_text(request, "XmlData")
        .filter(|value| !value.is_empty())
        .map(|value| {
            BASE64_STANDARD
                .decode(value.as_bytes())
                .map_err(|_| anyhow!("XmlData must be valid base64."))
                .and_then(|value| {
                    String::from_utf8(value)
                        .map_err(|_| anyhow!("XmlData must decode to UTF-8 XML text."))
                })
        })
        .transpose()?;
    let binary_payload = element_text(request, "BinaryData")
        .filter(|value| !value.is_empty())
        .map(|value| {
            BASE64_STANDARD
                .decode(value.as_bytes())
                .map_err(|_| anyhow!("BinaryData must be valid base64."))
        })
        .transpose()?;
    let payload_size = xml_payload.as_ref().map_or(0, |value| value.len())
        + binary_payload.as_ref().map_or(0, Vec::len);
    if payload_size > MAX_USER_CONFIGURATION_PAYLOAD_BYTES {
        bail!("User configuration payload exceeds the supported limit.");
    }
    Ok(UpsertEwsUserConfigurationInput {
        account_id: principal.account_id,
        key,
        dictionary_json,
        xml_payload,
        binary_payload,
    })
}

fn ews_user_configuration_name_xml(configuration: &EwsUserConfiguration) -> String {
    let name = escape_xml(&configuration.config_name);
    match configuration.scope_kind.as_str() {
        "account" => format!("<t:UserConfigurationName Name=\"{name}\"/>"),
        "mailbox" => configuration.mailbox_id.map_or_else(
            || format!("<t:UserConfigurationName Name=\"{name}\"/>"),
            |mailbox_id| format!(
                "<t:UserConfigurationName Name=\"{name}\"><t:FolderId Id=\"mailbox:{mailbox_id}\"/></t:UserConfigurationName>"
            ),
        ),
        "public_folder" => configuration.public_folder_id.map_or_else(
            || format!("<t:UserConfigurationName Name=\"{name}\"/>"),
            |public_folder_id| format!(
                "<t:UserConfigurationName Name=\"{name}\"><t:FolderId Id=\"public-folder:{public_folder_id}\"/></t:UserConfigurationName>"
            ),
        ),
        _ => format!("<t:UserConfigurationName Name=\"{name}\"/>"),
    }
}

fn parse_ews_user_configuration_dictionary(request: &str) -> Result<serde_json::Value> {
    let Some(dictionary) = element_content(request, "Dictionary") else {
        return Ok(serde_json::json!({}));
    };
    let mut object = serde_json::Map::new();
    for entry in element_contents(dictionary, "DictionaryEntry") {
        if object.len() == MAX_USER_CONFIGURATION_DICTIONARY_ENTRIES {
            bail!("Dictionary exceeds the supported entry limit.");
        }
        let key_type = element_content(entry, "DictionaryKey")
            .and_then(|content| element_text(content, "Type"));
        let value_type = element_content(entry, "DictionaryValue")
            .and_then(|content| element_text(content, "Type"));
        if key_type.as_deref() != Some("String") || value_type.as_deref() != Some("String") {
            bail!("Dictionary supports only String keys and values.");
        }
        let key = element_content(entry, "DictionaryKey")
            .and_then(|content| element_text(content, "Value"))
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow!("DictionaryEntry requires DictionaryKey Value."))?;
        let value = element_content(entry, "DictionaryValue")
            .and_then(|content| element_text(content, "Value"))
            .unwrap_or_default();
        if key.len() > MAX_USER_CONFIGURATION_DICTIONARY_KEY_BYTES
            || value.len() > MAX_USER_CONFIGURATION_DICTIONARY_VALUE_BYTES
        {
            bail!("Dictionary key or value exceeds the supported limit.");
        }
        if object.contains_key(&key) {
            bail!("Dictionary contains a duplicate key.");
        }
        object.insert(key, serde_json::Value::String(value));
    }
    Ok(serde_json::Value::Object(object))
}
