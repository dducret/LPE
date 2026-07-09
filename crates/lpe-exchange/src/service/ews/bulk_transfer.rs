use super::super::*;

impl<S, V> ExchangeService<S, V>
where
    S: ExchangeStore + Clone + Send + Sync + 'static,
    V: Detector + Clone + Send + Sync + 'static,
{
    pub(in crate::service) async fn upload_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let item_ids = requested_transfer_item_ids(request);
        if item_ids.is_empty() {
            return Ok(operation_error_response(
                "UploadItems",
                "ErrorInvalidOperation",
                "UploadItems requires explicit canonical ItemId or SourceItemId values; arbitrary Exchange item packages are not imported.",
            ));
        }
        let job = self
            .store
            .create_ews_transfer_job(
                principal,
                "import",
                &item_ids,
                serde_json::json!({ "operation": "UploadItems", "itemCount": item_ids.len() }),
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-upload-items".to_string(),
                    subject: format!("{} items", item_ids.len()),
                },
            )
            .await?;
        Ok(transfer_job_response("UploadItems", &job))
    }

    pub(in crate::service) async fn export_items(
        &self,
        principal: &AccountPrincipal,
        request: &str,
    ) -> Result<String> {
        let item_ids = requested_item_ids(request);
        if item_ids.is_empty() {
            return Ok(operation_error_response(
                "ExportItems",
                "ErrorInvalidOperation",
                "ExportItems requires at least one canonical ItemId.",
            ));
        }
        let job = self
            .store
            .create_ews_transfer_job(
                principal,
                "export",
                &item_ids,
                serde_json::json!({ "operation": "ExportItems", "itemCount": item_ids.len() }),
                AuditEntryInput {
                    actor: principal.email.clone(),
                    action: "ews-export-items".to_string(),
                    subject: format!("{} items", item_ids.len()),
                },
            )
            .await?;
        Ok(transfer_job_response("ExportItems", &job))
    }
}

pub(in crate::service) fn transfer_job_response(operation: &str, job: &EwsTransferJob) -> String {
    let entries_xml = job
        .entries
        .iter()
        .map(|entry| {
            format!(
                concat!(
                    "<t:TransferEntry>",
                    "<t:EntryId>{id}</t:EntryId>",
                    "<t:Ordinal>{ordinal}</t:Ordinal>",
                    "<t:ItemKind>{kind}</t:ItemKind>",
                    "<t:CanonicalId>{canonical_id}</t:CanonicalId>",
                    "<t:SourceItemId>{source_item_id}</t:SourceItemId>",
                    "<t:Status>{status}</t:Status>",
                    "</t:TransferEntry>"
                ),
                id = entry.id,
                ordinal = entry.ordinal,
                kind = escape_xml(&entry.item_kind),
                canonical_id = entry
                    .canonical_id
                    .map(|id| id.to_string())
                    .unwrap_or_default(),
                source_item_id = escape_xml(entry.source_item_id.as_deref().unwrap_or_default()),
                status = escape_xml(&entry.status),
            )
        })
        .collect::<String>();
    format!(
        concat!(
            "<m:{operation}Response>",
            "<m:ResponseMessages>",
            "<m:{operation}ResponseMessage ResponseClass=\"Success\">",
            "<m:ResponseCode>NoError</m:ResponseCode>",
            "<m:JobId>{job_id}</m:JobId>",
            "<m:Direction>{direction}</m:Direction>",
            "<m:Status>{status}</m:Status>",
            "<m:TotalItems>{total}</m:TotalItems>",
            "<m:TransferEntries>{entries_xml}</m:TransferEntries>",
            "</m:{operation}ResponseMessage>",
            "</m:ResponseMessages>",
            "</m:{operation}Response>"
        ),
        operation = operation,
        job_id = job.id,
        direction = escape_xml(&job.direction),
        status = escape_xml(&job.status),
        total = job.total_items,
        entries_xml = entries_xml,
    )
}
