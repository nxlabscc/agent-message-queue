import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createSqliteQueue } from "./sqlite";
import { createWorker } from "./worker";

type P = { n: number };
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
const newQueue = () => createSqliteQueue<P>(new Database(":memory:"));

async function until(cond: () => boolean | Promise<boolean>, ms = 2000) {
  const end = Date.now() + ms;
  while (!(await cond())) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await wait(5);
  }
}

test("concurrency 1 handles one message at a time, across topics", async () => {
  const q = newQueue();
  for (const t of ["a", "b", "c"]) for (const n of [1, 2]) await q.enqueue(t, { n });
  let active = 0, peak = 0, done = 0;
  const w = createWorker(q, {
    concurrency: 1, leaseMs: 1000, pollMs: 5, onFailure: "release",
    handler: async () => {
      peak = Math.max(peak, ++active);
      await wait(10);
      active--; done++;
    },
  });
  w.start();
  await until(() => done === 6);
  await w.stop();
  expect(peak).toBe(1);
  expect(await q.pendingTopics()).toEqual([]);
});

test("concurrency 2 runs two topics at once, never two of one topic", async () => {
  const q = newQueue();
  for (const t of ["a", "b"]) for (const n of [1, 2, 3]) await q.enqueue(t, { n });
  const activeByTopic = new Map<string, number>();
  let peak = 0, peakPerTopic = 0, done = 0;
  const order: string[] = [];
  const w = createWorker(q, {
    concurrency: 2, leaseMs: 1000, pollMs: 5, onFailure: "release",
    handler: async (m) => {
      const n = (activeByTopic.get(m.topic) ?? 0) + 1;
      activeByTopic.set(m.topic, n);
      peakPerTopic = Math.max(peakPerTopic, n);
      peak = Math.max(peak, [...activeByTopic.values()].reduce((s, v) => s + v, 0));
      await wait(15);
      order.push(`${m.topic}${m.payload.n}`);
      activeByTopic.set(m.topic, activeByTopic.get(m.topic)! - 1);
      done++;
    },
  });
  w.start();
  await until(() => done === 6);
  await w.stop();
  expect(peak).toBe(2);
  expect(peakPerTopic).toBe(1);
  // Each topic still in order.
  expect(order.filter((s) => s.startsWith("a"))).toEqual(["a1", "a2", "a3"]);
  expect(order.filter((s) => s.startsWith("b"))).toEqual(["b1", "b2", "b3"]);
});

test("stop waits for the message in hand, and acks it", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  let finished = false;
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5, onFailure: "release",
    handler: async () => { await wait(50); finished = true; },
  });
  w.start();
  await until(async () => (await q.topics())[0]?.inFlight === 1);
  await w.stop();
  expect(finished).toBe(true);
  expect(await q.pendingTopics()).toEqual([]);
});

test("a failing handler with onFailure 'ack' consumes the message", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  let calls = 0;
  const errors: unknown[] = [];
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5, onFailure: "ack",
    onError: (e) => errors.push(e),
    handler: async () => { calls++; throw new Error("boom"); },
  });
  w.start();
  await until(() => errors.length === 1);
  await wait(30);
  await w.stop();
  expect(calls).toBe(1);
  expect(await q.pendingTopics()).toEqual([]);
});

test("a failing handler with onFailure 'release' gets the message again", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  const attempts: number[] = [];
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5, onFailure: "release",
    handler: async (m) => {
      attempts.push(m.attempts);
      if (attempts.length === 1) throw new Error("first time fails");
    },
  });
  w.start();
  await until(() => attempts.length === 2);
  await w.stop();
  expect(attempts).toEqual([1, 2]);
  expect(await q.pendingTopics()).toEqual([]);
});

test("onFailure as a function decides per failure", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  await q.enqueue("b", { n: 2 });
  const seen: string[] = [];
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5,
    onFailure: (_err, m) => (m.topic === "a" ? "ack" : "release"),
    handler: async (m) => {
      seen.push(`${m.topic}${m.attempts}`);
      if (m.attempts === 1) throw new Error("first attempt fails");
    },
  });
  w.start();
  await until(() => seen.includes("b2"));
  await w.stop();
  expect(seen.filter((s) => s.startsWith("a"))).toEqual(["a1"]);   // acked, gone
  expect(await q.pendingTopics()).toEqual([]);
});

test("a handler longer than its lease keeps the message", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  let running = false;
  const w = createWorker(q, {
    leaseMs: 40, pollMs: 5, onFailure: "release",
    handler: async () => { running = true; await wait(150); },
  });
  w.start();
  await until(() => running);
  await wait(100);                                          // well past one lease
  expect(await q.claim({ leaseMs: 1000 })).toBeNull();      // still held
  await w.stop();
  expect(await q.pendingTopics()).toEqual([]);
});

test("a worker limited to some topics leaves the rest", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  await q.enqueue("b", { n: 2 });
  const seen: string[] = [];
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5, onFailure: "release", topics: () => ["b"],
    handler: async (m) => { seen.push(m.topic); },
  });
  w.start();
  await until(() => seen.length === 1);
  await wait(30);
  await w.stop();
  expect(seen).toEqual(["b"]);
  expect(await q.pendingTopics()).toEqual(["a"]);
});

test("a gate holds every claim while it answers a wait, and lets go at 0", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  let hold = 1_000;
  let done = 0;
  const w = createWorker(q, {
    concurrency: 1, leaseMs: 1000, pollMs: 5, onFailure: "release",
    gate: () => hold,
    handler: async () => { done++; },
  });
  w.start();
  await wait(40);
  expect(done).toBe(0);                 // paused: nothing taken
  hold = 0;
  await until(() => done === 1);
  await w.stop();
});

test("the hooks see claim, handled and done, in that order, on success and on failure", async () => {
  const q = newQueue();
  await q.enqueue("a", { n: 1 });
  await q.enqueue("b", { n: 2 });
  const events: string[] = [];
  const w = createWorker(q, {
    leaseMs: 1000, pollMs: 5, onFailure: "ack",
    handler: async (m) => {
      events.push(`handle ${m.payload.n}`);
      if (m.payload.n === 2) throw new Error("boom");
    },
    onClaim: (m) => events.push(`claim ${m.payload.n}`),
    onHandled: (m) => events.push(`handled ${m.payload.n}`),
    onDone: (m) => events.push(`done ${m.payload.n}`),
  });
  w.start();
  await until(() => events.length === 8);
  await w.stop();
  expect(events).toEqual([
    "claim 1", "handle 1", "handled 1", "done 1",
    "claim 2", "handle 2", "handled 2", "done 2",
  ]);
  expect(await q.pendingTopics()).toEqual([]);
});
