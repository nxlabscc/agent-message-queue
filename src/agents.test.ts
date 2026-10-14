import { test, expect } from "bun:test";
import { Database } from "bun:sqlite";
import { createSqliteQueue } from "./sqlite";
import {
  AlreadyQueued, createAgentMessaging, type AgentMessagingOptions, type AgentPhase,
} from "./agents";
import type { Message } from "./types";

type P = { to: string; n: number };
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function until(cond: () => boolean | Promise<boolean>, ms = 3000) {
  const end = Date.now() + ms;
  while (!(await cond())) {
    if (Date.now() > end) throw new Error("condition not met in time");
    await wait(5);
  }
}

function setup(over: Partial<AgentMessagingOptions<P>> = {}) {
  const queue = createSqliteQueue<P>(new Database(":memory:"));
  const started: string[] = [];
  const agents = createAgentMessaging<P>({
    queue,
    agentOf: (p) => p.to,
    isBusy: () => false,
    startSession: async (m) => { started.push(`${m.topic} ${m.payload.to} ${m.payload.n}`); return { errored: false }; },
    leaseMs: 1000,
    pollMs: 5,
    busyPollMs: 5,
    ...over,
  });
  return { queue, agents, started };
}

test("a topic runs one message at a time, oldest first, and acks each", async () => {
  let active = 0, peak = 0;
  const { queue, agents, started } = setup({
    startSession: async (m) => {
      peak = Math.max(peak, ++active);
      await wait(10);
      started.push(`${m.payload.to} ${m.payload.n}`);
      active--;
      return { errored: false };
    },
  });
  for (const [to, n] of [["a", 1], ["b", 2], ["c", 3]] as const) await agents.send("t", { to, n });
  agents.start(["t"]);
  await until(() => started.length === 3);
  await agents.stop();
  expect(started).toEqual(["a 1", "b 2", "c 3"]);
  expect(peak).toBe(1);
  expect(await queue.pendingTopics()).toEqual([]);
});

test("different topics run beside each other for different agents", async () => {
  let active = 0, peak = 0;
  const { agents, started } = setup({
    startSession: async (m) => {
      peak = Math.max(peak, ++active);
      await wait(30);
      started.push(m.payload.to);
      active--;
      return { errored: false };
    },
  });
  await agents.send("x", { to: "a", n: 1 });
  await agents.send("y", { to: "b", n: 2 });
  agents.start(["x", "y"]);
  await until(() => started.length === 2);
  await agents.stop();
  expect(peak).toBe(2);
});

test("an agent never has two sessions open, even from two topics", async () => {
  const open = new Map<string, number>();
  let peakPerAgent = 0;
  const { agents, started } = setup({
    startSession: async (m) => {
      const n = (open.get(m.payload.to) ?? 0) + 1;
      open.set(m.payload.to, n);
      peakPerAgent = Math.max(peakPerAgent, n);
      await wait(30);
      open.set(m.payload.to, n - 1);
      started.push(`${m.topic} ${m.payload.to}`);
      return { errored: false };
    },
  });
  await agents.send("x", { to: "galmar", n: 1 });
  await agents.send("y", { to: "galmar", n: 2 });
  await agents.send("z", { to: "galmar", n: 3 });
  agents.start(["x", "y", "z"]);
  await until(() => started.length === 3);
  await agents.stop();
  expect(peakPerAgent).toBe(1);
  expect(started.map((s) => s.split(" ")[1])).toEqual(["galmar", "galmar", "galmar"]);
});

test("an agent busy elsewhere is waited for: blocked, then running", async () => {
  const seen: (AgentPhase | null)[] = [];
  let busyFor = 3;
  let id = "";
  const { agents, started } = setup({
    isBusy: () => {
      if (id) seen.push(agents.phaseOf(id));
      return busyFor-- > 0;
    },
    startSession: async (m) => {
      seen.push(agents.phaseOf(m.id));
      started.push(m.payload.to);
      return { errored: false };
    },
  });
  id = (await agents.send("t", { to: "tolfdir", n: 1 })).id;
  agents.start(["t"]);
  await until(() => started.length === 1);
  await agents.stop();
  expect(seen.at(-1)).toBe("running");
  expect(seen).toContain("blocked");
  expect(seen.indexOf("blocked")).toBeLessThan(seen.indexOf("running"));
});

test("a start that loses the race to another session waits and starts again", async () => {
  let calls = 0;
  const { agents, queue } = setup({
    startSession: async () => (++calls === 1 ? null : { errored: false }),
  });
  await agents.send("t", { to: "a", n: 1 });
  agents.start(["t"]);
  await until(() => calls === 2);
  await agents.stop();
  expect(await queue.pendingTopics()).toEqual([]);
});

test("a session that errored or threw is consumed and reported, not run again", async () => {
  const errors: string[] = [];
  let calls = 0;
  const { agents, queue } = setup({
    startSession: async (m) => {
      calls++;
      if (m.payload.n === 1) return { errored: true };
      throw new Error("boom");
    },
    onError: (err, m) => errors.push(`${m.payload.n} ${(err as Error).message.includes("errored") ? "errored" : "threw"}`),
  });
  await agents.send("t", { to: "a", n: 1 });
  await agents.send("t", { to: "a", n: 2 });
  agents.start(["t"]);
  await until(() => errors.length === 2);
  await wait(30);
  await agents.stop();
  expect(calls).toBe(2);
  expect(errors).toEqual(["1 errored", "2 threw"]);
  expect(await queue.pendingTopics()).toEqual([]);
});

test("a stop while waiting on a busy agent puts the message back, untouched and unreported", async () => {
  let checks = 0;
  const errors: unknown[] = [];
  const { agents, queue, started } = setup({
    isBusy: () => { checks++; return true; },
    onError: (err) => errors.push(err),
  });
  await agents.send("t", { to: "a", n: 1 });
  agents.start(["t"]);
  await until(() => checks >= 2);
  await agents.stop();
  expect(started).toEqual([]);
  expect(errors).toEqual([]);
  const [m] = await queue.list("t");
  expect(m!.state).toBe("pending");
  expect(m!.payload).toEqual({ to: "a", n: 1 });
});

test("the view shows each message's agent, its place in line, and what its worker is doing", async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  const { agents } = setup({
    startSession: async () => { await gate; return { errored: false }; },
  });
  await agents.send("t", { to: "tolfdir", n: 1 });
  await agents.send("t", { to: "ancano", n: 2 });
  await agents.send("t", { to: "urag", n: 3 });

  const before = await agents.view(["t"]);
  expect(before.map((v) => [v.agent, v.state, v.position])).toEqual([
    ["tolfdir", "waiting", 1], ["ancano", "waiting", 2], ["urag", "waiting", 3],
  ]);

  agents.start(["t"]);
  await until(async () => (await agents.view(["t"]))[0]!.state === "running");
  const during = await agents.view(["t"]);
  expect(during.map((v) => [v.agent, v.state, v.position])).toEqual([
    ["tolfdir", "running", null], ["ancano", "waiting", 1], ["urag", "waiting", 2],
  ]);
  release();
  await until(async () => (await agents.view(["t"])).length === 0);
  await agents.stop();
});

test("a message held by another process reads taken", async () => {
  const { agents, queue } = setup();
  await agents.send("t", { to: "a", n: 1 });
  await queue.claim({ topics: ["t"], leaseMs: 60_000 });
  const [v] = await agents.view(["t"]);
  expect(v!.state).toBe("taken");
  expect(v!.position).toBeNull();
});

test("send with a key refuses a message already queued, waiting or in flight", async () => {
  const { agents, queue } = setup();
  const key = (p: P) => `${p.to}-${p.n}`;
  await agents.send("t", { to: "a", n: 1 }, { key });
  await expect(agents.send("t", { to: "a", n: 1 }, { key })).rejects.toBeInstanceOf(AlreadyQueued);
  await queue.claim({ topics: ["t"], leaseMs: 60_000 });
  await expect(agents.send("t", { to: "a", n: 1 }, { key })).rejects.toBeInstanceOf(AlreadyQueued);
  await agents.send("t", { to: "a", n: 2 }, { key });
  await agents.send("other", { to: "a", n: 1 }, { key });
  expect((await queue.list("t")).length).toBe(2);
});

test("a gate holds every claim while it answers a wait", async () => {
  let hold = 1_000;
  const { agents, started } = setup({ gate: () => hold });
  await agents.send("t", { to: "a", n: 1 });
  agents.start(["t"]);
  await wait(40);
  expect(started).toEqual([]);
  hold = 0;
  await until(() => started.length === 1);
  await agents.stop();
});

test("startSession receives the whole message", async () => {
  let got: Message<P> | null = null;
  const { agents } = setup({ startSession: async (m) => { got = m; return { errored: false }; } });
  const sent = await agents.send("t", { to: "a", n: 7 });
  agents.start(["t"]);
  await until(() => got !== null);
  await agents.stop();
  expect(got!.id).toBe(sent.id);
  expect(got!.topic).toBe("t");
  expect(got!.payload).toEqual({ to: "a", n: 7 });
  expect(got!.attempts).toBe(1);
});
