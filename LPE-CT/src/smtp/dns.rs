use anyhow::{Context, Result};
use email_auth::common::dns::{DnsError, DnsResolver, MxRecord};
use hickory_resolver::{proto::rr::RecordType, TokioResolver};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Clone)]
pub(in crate::smtp) struct SystemDnsResolver {
    resolver: TokioResolver,
}

impl SystemDnsResolver {
    pub(in crate::smtp) fn new() -> Result<Self> {
        let resolver = TokioResolver::builder_tokio()
            .context("unable to create DNS resolver builder from system configuration")?
            .build();
        Ok(Self { resolver })
    }
}

impl DnsResolver for SystemDnsResolver {
    async fn query_txt(&self, name: &str) -> Result<Vec<String>, DnsError> {
        let lookup = self
            .resolver
            .lookup(name, RecordType::TXT)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.to_string()).collect())
    }

    async fn query_a(&self, name: &str) -> Result<Vec<Ipv4Addr>, DnsError> {
        let lookup = self
            .resolver
            .ipv4_lookup(name)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.0).collect())
    }

    async fn query_aaaa(&self, name: &str) -> Result<Vec<Ipv6Addr>, DnsError> {
        let lookup = self
            .resolver
            .ipv6_lookup(name)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|record| record.0).collect())
    }

    async fn query_mx(&self, name: &str) -> Result<Vec<MxRecord>, DnsError> {
        let lookup = self.resolver.mx_lookup(name).await.map_err(map_dns_error)?;
        Ok(lookup
            .iter()
            .map(|record| MxRecord {
                preference: record.preference(),
                exchange: record.exchange().to_utf8(),
            })
            .collect())
    }

    async fn query_ptr(&self, ip: &IpAddr) -> Result<Vec<String>, DnsError> {
        let lookup = self
            .resolver
            .reverse_lookup(*ip)
            .await
            .map_err(map_dns_error)?;
        Ok(lookup.iter().map(|name| name.to_utf8()).collect())
    }

    async fn query_exists(&self, name: &str) -> Result<bool, DnsError> {
        let a = self.resolver.lookup_ip(name).await.map_err(map_dns_error)?;
        Ok(a.iter().next().is_some())
    }
}

fn map_dns_error(error: hickory_resolver::ResolveError) -> DnsError {
    let message = error.to_string().to_ascii_lowercase();
    if message.contains("nxdomain") || message.contains("no such domain") {
        DnsError::NxDomain
    } else if message.contains("no records") || message.contains("no data") {
        DnsError::NoRecords
    } else {
        DnsError::TempFail
    }
}
