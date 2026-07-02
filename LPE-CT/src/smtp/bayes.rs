use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct BayesCorpus {
    pub(crate) ham_messages: u32,
    pub(crate) spam_messages: u32,
    pub(crate) ham_tokens: HashMap<String, u32>,
    pub(crate) spam_tokens: HashMap<String, u32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BayesOutcome {
    pub(crate) probability: f32,
    pub(crate) matched_tokens: usize,
    pub(crate) contribution: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BayesLabel {
    Ham,
    Spam,
}

pub(crate) const BAYESPAM_MIN_SCORING_TOKENS: usize = 3;

pub(crate) async fn load_bayespam_corpus(
    spool_dir: &Path,
    config: &RuntimeConfig,
) -> Result<BayesCorpus> {
    if let Some(pool) = ensure_local_db_schema(config).await? {
        let row = sqlx::query("SELECT corpus FROM bayespam_corpora WHERE corpus_key = $1")
            .bind("default")
            .fetch_optional(pool)
            .await?;
        return Ok(row
            .map(|row| row.try_get::<Json<BayesCorpus>, _>("corpus"))
            .transpose()?
            .map(|value| value.0)
            .unwrap_or_default());
    }

    let path = spool_dir.join("policy").join("bayespam.json");
    if !path.exists() {
        return Ok(BayesCorpus::default());
    }
    Ok(serde_json::from_str(&fs::read_to_string(path)?)?)
}

async fn save_bayespam_corpus(
    spool_dir: &Path,
    config: &RuntimeConfig,
    corpus: &BayesCorpus,
) -> Result<()> {
    if let Some(pool) = ensure_local_db_schema(config).await? {
        sqlx::query(
            r#"
            INSERT INTO bayespam_corpora (corpus_key, corpus, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (corpus_key) DO UPDATE SET
                corpus = EXCLUDED.corpus,
                updated_at = NOW()
            "#,
        )
        .bind("default")
        .bind(Json(corpus))
        .execute(pool)
        .await?;
        return Ok(());
    }

    let path = spool_dir.join("policy").join("bayespam.json");
    fs::write(path, serde_json::to_string_pretty(corpus)?)?;
    Ok(())
}

fn tokenize_for_bayespam(
    subject: &str,
    visible_text: &str,
    min_token_length: usize,
    max_tokens: usize,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut tokens = Vec::new();
    for token in [subject, visible_text].into_iter().flat_map(|value| {
        value
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .map(str::trim)
            .filter(|token| token.len() >= min_token_length)
            .map(|token| token.to_ascii_lowercase())
            .collect::<Vec<_>>()
    }) {
        if seen.insert(token.clone()) {
            tokens.push(token);
            if tokens.len() >= max_tokens {
                break;
            }
        }
    }
    tokens
}

fn bayespam_token_probability(corpus: &BayesCorpus, token: &str) -> Option<f64> {
    if corpus.ham_messages == 0 || corpus.spam_messages == 0 {
        return None;
    }
    let spam_count = *corpus.spam_tokens.get(token).unwrap_or(&0);
    let ham_count = *corpus.ham_tokens.get(token).unwrap_or(&0);
    if spam_count == 0 && ham_count == 0 {
        return None;
    }
    let spam = (spam_count as f64 + 1.0) / (corpus.spam_messages as f64 + 2.0);
    let ham = (ham_count as f64 + 1.0) / (corpus.ham_messages as f64 + 2.0);
    let probability = spam / (spam + ham);
    Some(probability.clamp(0.01, 0.99))
}

fn score_bayespam_tokens(
    corpus: &BayesCorpus,
    tokens: &[String],
    score_weight: f32,
) -> Option<BayesOutcome> {
    if corpus.ham_messages == 0 || corpus.spam_messages == 0 {
        return None;
    }

    let mut log_spam = 0.0f64;
    let mut log_ham = 0.0f64;
    let mut matched = 0usize;
    for token in tokens {
        let Some(probability) = bayespam_token_probability(corpus, token) else {
            continue;
        };
        log_spam += probability.ln();
        log_ham += (1.0 - probability).ln();
        matched += 1;
    }

    if matched == 0 {
        return None;
    }
    if matched < BAYESPAM_MIN_SCORING_TOKENS {
        return Some(BayesOutcome {
            probability: 0.5,
            matched_tokens: matched,
            contribution: 0.0,
        });
    }

    let probability = 1.0 / (1.0 + (log_ham - log_spam).exp());
    let contribution = ((probability as f32 - 0.5).max(0.0) * 2.0) * score_weight.max(0.0);
    Some(BayesOutcome {
        probability: probability as f32,
        matched_tokens: matched,
        contribution,
    })
}

pub(crate) async fn score_bayespam(
    spool_dir: &Path,
    config: &RuntimeConfig,
    subject: &str,
    visible_text: &str,
    _mail_from: &str,
    _helo: &str,
) -> Result<Option<BayesOutcome>> {
    if !config.bayespam_enabled {
        return Ok(None);
    }
    let corpus = load_bayespam_corpus(spool_dir, config).await?;
    let tokens = tokenize_for_bayespam(
        subject,
        visible_text,
        config.bayespam_min_token_length.max(2) as usize,
        config.bayespam_max_tokens.max(16) as usize,
    );
    Ok(score_bayespam_tokens(
        &corpus,
        &tokens,
        config.bayespam_score_weight,
    ))
}

pub(in crate::smtp) async fn train_bayespam(
    spool_dir: &Path,
    config: &RuntimeConfig,
    message: &QueuedMessage,
    label: BayesLabel,
) -> Result<()> {
    if !config.bayespam_enabled || !config.bayespam_auto_learn {
        return Ok(());
    }

    let subject = parse_rfc822_header_value(&message.data, "subject").unwrap_or_default();
    let visible_text = extract_visible_text(&message.data)?;
    let tokens = tokenize_for_bayespam(
        &subject,
        &visible_text,
        config.bayespam_min_token_length.max(2) as usize,
        config.bayespam_max_tokens.max(16) as usize,
    );
    if tokens.is_empty() {
        return Ok(());
    }

    let mut corpus = load_bayespam_corpus(spool_dir, config).await?;
    match label {
        BayesLabel::Ham => {
            corpus.ham_messages = corpus.ham_messages.saturating_add(1);
            for token in tokens {
                let entry = corpus.ham_tokens.entry(token).or_insert(0);
                *entry = entry.saturating_add(1);
            }
        }
        BayesLabel::Spam => {
            corpus.spam_messages = corpus.spam_messages.saturating_add(1);
            for token in tokens {
                let entry = corpus.spam_tokens.entry(token).or_insert(0);
                *entry = entry.saturating_add(1);
            }
        }
    }
    save_bayespam_corpus(spool_dir, config, &corpus).await
}
