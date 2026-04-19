ALTER TABLE outbound_message_queue
    ADD COLUMN IF NOT EXISTS remote_message_ref TEXT;
