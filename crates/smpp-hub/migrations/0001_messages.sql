CREATE TABLE messages (
    id TEXT PRIMARY KEY NOT NULL,
    received_at INTEGER NOT NULL,
    receipt_for TEXT UNIQUE,
    payload TEXT NOT NULL,
    remote_id TEXT,
    handset_state INTEGER
);
CREATE INDEX messages_remote_id ON messages(remote_id);
