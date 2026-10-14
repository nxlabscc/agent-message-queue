import type { Database } from "bun:sqlite";
import { ulid } from "./ulid";
import type { Claim, Message, MessageView, Queue, QueueInspector, TopicStats } from "./types";

// The SQLite backend. It owns its table and creates it on construction, so the
// same code runs against the runtime's database and against `:memory:` in tests.
//
// `seq` is the order: an autoincrement, so two messages enqueued in the same
// millisecond still have a first and a second. `id` is what callers see.
//
// A message is in flight while `lease_until` is in the future. Nothing clears a
// lapsed lease: the next claim simply treats it as available.

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS queue_messages (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    id           TEXT    NOT NULL UNIQUE,
    topic        TEXT    NOT NULL,
    payload      TEXT    NOT NULL,
    enqueued_at  INTEGER NOT NULL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    lease_token  TEXT,
    lease_until  INTEGER,
    claimed_at   INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_queue_messages_topic ON queue_messages (topic, seq);
`;

type Row = {
  seq: number;
  id: string;
  topic: string;
  payload: string;
  enqueued_at: number;
  attempts: number;
  lease_token: string | null;
  lease_until: number | null;
  claimed_at: number | null;
};

export type SqliteQueueOptions = {
  /** The clock every lease is measured against. Tests pass their own. */
  now?: () => number;
};

export function createSqliteQueue<P>(
  db: Database,
  opts: SqliteQueueOptions = {},
): Queue<P> & QueueInspector<P> {
  const now = opts.now ?? Date.now;
  db.exec(SCHEMA);

  const toMessage = (r: Row): Message<P> => ({
    id: r.id,
    topic: r.topic,
    payload: JSON.parse(r.payload) as P,
    enqueuedAt: r.enqueued_at,
    attempts: r.attempts,
  });

  const toView = (r: Row, at: number): MessageView<P> => {
    const inFlight = r.lease_until !== null && r.lease_until > at;
    return {
      ...toMessage(r),
      state: inFlight ? "in_flight" : "pending",
      claimedAt: inFlight ? r.claimed_at : null,
      leaseUntil: inFlight ? r.lease_until : null,
    };
  };

  const insert = db.prepare(
    "INSERT INTO queue_messages (id, topic, payload, enqueued_at) VALUES (?, ?, ?, ?) RETURNING *",
  );
  const byId = db.prepare("SELECT * FROM queue_messages WHERE id = ?");
  const extendStmt = db.prepare(
    "UPDATE queue_messages SET lease_until = ? WHERE id = ? AND lease_token = ? AND lease_until > ?",
  );
  const ackStmt = db.prepare(
    "DELETE FROM queue_messages WHERE id = ? AND lease_token = ? AND lease_until > ?",
  );
  const releaseStmt = db.prepare(
    `UPDATE queue_messages SET lease_token = NULL, lease_until = NULL, claimed_at = NULL
      WHERE id = ? AND lease_token = ? AND lease_until > ?`,
  );

  // The oldest message whose topic has nothing else in flight. With nothing in
  // flight in a topic, its oldest message is its head, so FIFO per topic holds.
  const claimOne = (topics: string[] | undefined, leaseMs: number): Row | null => {
    const at = now();
    const filter = topics ? `AND m.topic IN (${topics.map(() => "?").join(", ")})` : "";
    const stmt = db.prepare(`
      UPDATE queue_messages
         SET lease_token = ?, lease_until = ?, claimed_at = ?, attempts = attempts + 1
       WHERE seq = (
         SELECT m.seq FROM queue_messages m
          WHERE (m.lease_until IS NULL OR m.lease_until <= ?)
            ${filter}
            AND NOT EXISTS (
              SELECT 1 FROM queue_messages o
               WHERE o.topic = m.topic AND o.seq != m.seq AND o.lease_until > ?
            )
          ORDER BY m.seq
          LIMIT 1
       )
      RETURNING *
    `);
    return (stmt.get(ulid(at), at + leaseMs, at, at, ...(topics ?? []), at) as Row | null) ?? null;
  };
  // IMMEDIATE takes the write lock before the SELECT inside the UPDATE runs, so
  // two processes claiming at once cannot both pick the same row.
  const claimTx = db.transaction(claimOne);

  return {
    async enqueue(topic, payload) {
      const row = insert.get(ulid(now()), topic, JSON.stringify(payload), now()) as Row;
      return toMessage(row);
    },

    async peek(topic, limit = 10) {
      const at = now();
      const rows = db.prepare(`
        SELECT * FROM queue_messages
         WHERE topic = ? AND (lease_until IS NULL OR lease_until <= ?)
         ORDER BY seq LIMIT ?
      `).all(topic, at, limit) as Row[];
      return rows.map(toMessage);
    },

    async pendingTopics() {
      const rows = db.prepare(
        "SELECT DISTINCT topic FROM queue_messages ORDER BY topic",
      ).all() as { topic: string }[];
      return rows.map((r) => r.topic);
    },

    async claim({ topics, leaseMs }) {
      if (topics && topics.length === 0) return null;
      const row = claimTx.immediate(topics, leaseMs);
      if (!row) return null;
      return { message: toMessage(row), token: row.lease_token!, leaseUntil: row.lease_until! };
    },

    async extend(claim, leaseMs) {
      const at = now();
      const res = extendStmt.run(at + leaseMs, claim.message.id, claim.token, at);
      if (res.changes === 0) return false;
      claim.leaseUntil = at + leaseMs;
      return true;
    },

    async ack(claim) {
      return ackStmt.run(claim.message.id, claim.token, now()).changes > 0;
    },

    async release(claim) {
      return releaseStmt.run(claim.message.id, claim.token, now()).changes > 0;
    },

    // ── observability: reads only ──────────────────────────────────────────

    async topics() {
      const at = now();
      return db.prepare(`
        SELECT topic,
               SUM(CASE WHEN lease_until > ? THEN 0 ELSE 1 END) AS pending,
               SUM(CASE WHEN lease_until > ? THEN 1 ELSE 0 END) AS inFlight,
               MIN(enqueued_at) AS oldestEnqueuedAt
          FROM queue_messages
         GROUP BY topic
         ORDER BY topic
      `).all(at, at) as TopicStats[];
    },

    async list(topic, { limit = 100, offset = 0 } = {}) {
      const at = now();
      const rows = db.prepare(
        "SELECT * FROM queue_messages WHERE topic = ? ORDER BY seq LIMIT ? OFFSET ?",
      ).all(topic, limit, offset) as Row[];
      return rows.map((r) => toView(r, at));
    },

    async get(id) {
      const row = byId.get(id) as Row | null;
      return row ? toView(row, now()) : null;
    },
  };
}
