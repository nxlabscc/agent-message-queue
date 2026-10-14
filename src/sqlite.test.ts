import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createSqliteQueue } from "./sqlite";
import { queueContract } from "./contract";

queueContract("sqlite :memory:", (now) => createSqliteQueue(new Database(":memory:"), { now }));

test("two queues on one database file see the same messages", async () => {
  // What a second process sees: its own connection, the same rows.
  const path = `/tmp/queue-${crypto.randomUUID()}.db`;
  const a = createSqliteQueue<{ n: number }>(new Database(path, { create: true }));
  const b = createSqliteQueue<{ n: number }>(new Database(path));
  await a.enqueue("t", { n: 1 });
  const c = await b.claim({ leaseMs: 1000 });
  expect(c!.message.payload.n).toBe(1);
  expect(await a.claim({ leaseMs: 1000 })).toBeNull();
  expect(await a.ack(c!)).toBe(true);
});
