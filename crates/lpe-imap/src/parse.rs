use anyhow::{anyhow, bail, Result};

#[derive(Debug)]
pub(crate) struct RequestLine {
    pub(crate) tag: String,
    pub(crate) command: String,
    pub(crate) arguments: String,
}

pub(crate) fn parse_request_line(line: &str) -> Result<RequestLine> {
    let mut parts = line.splitn(3, ' ');
    let tag = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing command tag"))?;
    let command = parts
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("missing command"))?;
    Ok(RequestLine {
        tag: tag.to_string(),
        command: command.to_ascii_uppercase(),
        arguments: parts.next().unwrap_or_default().trim().to_string(),
    })
}

pub(crate) fn tokenize(input: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut cursor = 0usize;

    while cursor < chars.len() {
        while cursor < chars.len() && chars[cursor].is_whitespace() {
            cursor += 1;
        }
        if cursor >= chars.len() {
            break;
        }

        match chars[cursor] {
            '"' => {
                cursor += 1;
                let mut token = String::new();
                while cursor < chars.len() {
                    match chars[cursor] {
                        '"' => {
                            cursor += 1;
                            break;
                        }
                        '\\' if cursor + 1 < chars.len() => {
                            token.push(chars[cursor + 1]);
                            cursor += 2;
                        }
                        ch => {
                            token.push(ch);
                            cursor += 1;
                        }
                    }
                }
                tokens.push(token);
            }
            '(' => {
                let start = cursor;
                let mut depth = 0usize;
                while cursor < chars.len() {
                    match chars[cursor] {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                cursor += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    cursor += 1;
                }
                tokens.push(chars[start..cursor].iter().collect());
            }
            _ => {
                let start = cursor;
                while cursor < chars.len() && !chars[cursor].is_whitespace() {
                    cursor += 1;
                }
                tokens.push(chars[start..cursor].iter().collect());
            }
        }
    }

    Ok(tokens)
}

pub(crate) fn split_two(input: &str) -> Result<(&str, &str)> {
    let trimmed = input.trim();
    let Some(index) = trimmed.find(char::is_whitespace) else {
        bail!("invalid command syntax");
    };
    Ok((&trimmed[..index], trimmed[index..].trim()))
}

pub(crate) fn parse_literal_size(token: &str) -> Result<usize> {
    let value = token.trim().trim_start_matches('{').trim_end_matches('}');
    value.parse::<usize>().map_err(Into::into)
}

pub(crate) fn first_token(arguments: &str, error: &str) -> Result<String> {
    tokenize(arguments)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!(error.to_string()))
}
