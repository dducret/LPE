ALTER TABLE outbound_message_queue
    ADD COLUMN IF NOT EXISTS last_result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retry_after_seconds INTEGER,
    ADD COLUMN IF NOT EXISTS retry_policy TEXT,
    ADD COLUMN IF NOT EXISTS last_dsn_action TEXT,
    ADD COLUMN IF NOT EXISTS last_dsn_status TEXT,
    ADD COLUMN IF NOT EXISTS last_smtp_code INTEGER,
    ADD COLUMN IF NOT EXISTS last_enhanced_status TEXT,
    ADD COLUMN IF NOT EXISTS last_routing_rule TEXT,
    ADD COLUMN IF NOT EXISTS last_throttle_scope TEXT,
    ADD COLUMN IF NOT EXISTS last_throttle_delay_seconds INTEGER;
