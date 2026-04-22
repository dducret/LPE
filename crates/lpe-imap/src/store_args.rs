use anyhow::{bail, Result};
use std::collections::HashSet;

use crate::parse::tokenize;

#[derive(Clone, Copy)]
pub(crate) struct StoreMode {
    pub(crate) replace: bool,
    pub(crate) silent: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct StoreCondstore {
    pub(crate) unchanged_since: Option<u64>,
}

pub(crate) fn parse_store_arguments(
    input: &str,
) -> Result<(String, StoreCondstore, String, String)> {
    let tokens = tokenize(input)?;
    if tokens.len() < 3 {
        bail!("STORE expects a message set, mode, and flag list");
    }

    let set_token = tokens[0].clone();
    let mut cursor = 1usize;
    let mut condstore = StoreCondstore {
        unchanged_since: None,
    };
    if tokens[cursor]
        .to_ascii_uppercase()
        .starts_with("(UNCHANGEDSINCE ")
    {
        condstore = parse_store_condstore(&tokens[cursor])?;
        cursor += 1;
    }
    if tokens.len() != cursor + 2 {
        bail!("STORE expects a message set, mode, and flag list");
    }

    Ok((
        set_token,
        condstore,
        tokens[cursor].clone(),
        tokens[cursor + 1].clone(),
    ))
}

fn parse_store_condstore(token: &str) -> Result<StoreCondstore> {
    let source = token
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let parts = source.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 2 || !parts[0].eq_ignore_ascii_case("UNCHANGEDSINCE") {
        bail!("unsupported STORE modifier {}", token);
    }
    Ok(StoreCondstore {
        unchanged_since: Some(parts[1].parse::<u64>()?),
    })
}

pub(crate) fn parse_store_mode(token: &str) -> Result<StoreMode> {
    Ok(match token.to_ascii_uppercase().as_str() {
        "FLAGS" => StoreMode {
            replace: true,
            silent: false,
        },
        "FLAGS.SILENT" => StoreMode {
            replace: true,
            silent: true,
        },
        "+FLAGS" => StoreMode {
            replace: false,
            silent: false,
        },
        "+FLAGS.SILENT" => StoreMode {
            replace: false,
            silent: true,
        },
        "-FLAGS" => StoreMode {
            replace: false,
            silent: false,
        },
        "-FLAGS.SILENT" => StoreMode {
            replace: false,
            silent: true,
        },
        other => bail!("unsupported STORE mode {}", other),
    })
}

pub(crate) fn parse_flag_list(token: &str) -> Result<HashSet<String>> {
    let source = token
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim();
    let mut flags = HashSet::new();
    for item in source.split_whitespace() {
        flags.insert(item.to_string());
    }
    Ok(flags)
}
