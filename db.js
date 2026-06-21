'use strict';
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const db = new sqlite3.Database(path.join(__dirname, 'relay.db'));

// Run pragmas and table creation in one exec block
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;

  CREATE TABLE IF NOT EXISTS sessions (
    id         TEXT PRIMARY KEY,
    role       TEXT NOT NULL CHECK(role IN ('buyer','seller')),
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS orders (
    id          TEXT PRIMARY KEY,
    session_id  TEXT NOT NULL,
    buyer_name  TEXT,
    buyer_phone TEXT,
    items       TEXT NOT NULL,
    total       INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'PENDING'
                  CHECK(status IN ('PENDING','CONFIRMED','DISPATCHED','REJECTED')),
    sms_ref     TEXT,
    notes       TEXT,
    created_at  INTEGER NOT NULL DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS messages (
    id          TEXT PRIMARY KEY,
    thread_id   TEXT NOT NULL,
    sender_role TEXT NOT NULL CHECK(sender_role IN ('buyer','seller')),
    content     TEXT NOT NULL,
    created_at  INTEGER NOT NULL DEFAULT (unixepoch())
  );

  CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL
  );

  INSERT OR IGNORE INTO settings(key, value) VALUES ('shop_open', 'true');

  CREATE INDEX IF NOT EXISTS idx_orders_session   ON orders(session_id);
  CREATE INDEX IF NOT EXISTS idx_orders_created   ON orders(created_at);
  CREATE INDEX IF NOT EXISTS idx_messages_thread  ON messages(thread_id);
  CREATE INDEX IF NOT EXISTS idx_messages_created ON messages(created_at);
`);

module.exports = db;