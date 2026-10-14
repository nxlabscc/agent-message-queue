import { describe, test, expect } from "bun:test";
import type { Queue, QueueInspector } from "./types";

// What any backend must do. A backend's test file calls this with a factory;
// a Redis or Postgres backend added later runs these same cases.
//
// The factory gets the clock the queue must measure leases against, so lease
// expiry is tested by moving time rather than waiting for it.

type Payload = { n: number; note?: string };
export type QueueFactory = (now: () => number) => Queue<Payload> & QueueInspector<Payload>;

export function queueContract(name: string, make: QueueFactory): void {
  describe(`queue contract: ${name}`, () => {
    const setup = () => {
      const clock = { t: 1_000_000 };
      const q = make(() => clock.t);
      return { q, clock };
    };

    test("a message goes in and comes back out whole", async () => {
      const { q } = setup();
      const m = await q.enqueue("a", { n: 1, note: "héllo 'quoted' $5" });
      expect(m.topic).toBe("a");
      expect(m.attempts).toBe(0);
      const c = await q.claim({ leaseMs: 1000 });
      expect(c!.message.id).toBe(m.id);
      expect(c!.message.payload).toEqual({ n: 1, note: "héllo 'quoted' $5" });
    });

    test("a topic is first in, first out", async () => {
      const { q } = setup();
      for (const n of [1, 2, 3]) await q.enqueue("a", { n });
      expect((await q.peek("a")).map((m) => m.payload.n)).toEqual([1, 2, 3]);
      const c = await q.claim({ leaseMs: 1000 });
      expect(c!.message.payload.n).toBe(1);
    });

    test("peek claims nothing", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      await q.peek("a");
      await q.peek("a");
      const c = await q.claim({ leaseMs: 1000 });
      expect(c!.message.payload.n).toBe(1);
      expect(c!.message.attempts).toBe(1);
    });

    test("peek shows what a consumer could take next, not what is in flight", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      await q.enqueue("a", { n: 2 });
      await q.claim({ leaseMs: 1000 });
      expect((await q.peek("a")).map((m) => m.payload.n)).toEqual([2]);
    });

    test("one message in flight per topic; other topics still go", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      await q.enqueue("a", { n: 2 });
      await q.enqueue("b", { n: 3 });
      const first = await q.claim({ leaseMs: 1000 });
      const second = await q.claim({ leaseMs: 1000 });
      const third = await q.claim({ leaseMs: 1000 });
      expect(first!.message.payload.n).toBe(1);
      expect(second!.message.payload.n).toBe(3);   // a is busy, so b
      expect(third).toBeNull();                    // a still busy, b empty
    });

    test("two claims never hold the same message", async () => {
      const { q } = setup();
      for (const t of ["a", "b", "c"]) await q.enqueue(t, { n: 1 });
      const ids = new Set<string>();
      for (let i = 0; i < 3; i++) ids.add((await q.claim({ leaseMs: 1000 }))!.message.id);
      expect(ids.size).toBe(3);
      expect(await q.claim({ leaseMs: 1000 })).toBeNull();
    });

    test("claim can be limited to some topics", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      await q.enqueue("b", { n: 2 });
      expect((await q.claim({ topics: ["b"], leaseMs: 1000 }))!.message.topic).toBe("b");
      expect(await q.claim({ topics: ["b"], leaseMs: 1000 })).toBeNull();
      expect(await q.claim({ topics: [], leaseMs: 1000 })).toBeNull();
    });

    test("ack removes the message", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      const c = await q.claim({ leaseMs: 1000 });
      expect(await q.ack(c!)).toBe(true);
      expect(await q.pendingTopics()).toEqual([]);
      expect(await q.claim({ leaseMs: 1000 })).toBeNull();
    });

    test("ack frees the topic for its next message", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      await q.enqueue("a", { n: 2 });
      await q.ack((await q.claim({ leaseMs: 1000 }))!);
      expect((await q.claim({ leaseMs: 1000 }))!.message.payload.n).toBe(2);
    });

    test("a lapsed lease makes the message claimable again, and counts the attempt", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      const first = await q.claim({ leaseMs: 1000 });
      expect(await q.claim({ leaseMs: 1000 })).toBeNull();
      clock.t += 1000;
      const again = await q.claim({ leaseMs: 1000 });
      expect(again!.message.id).toBe(first!.message.id);
      expect(again!.message.attempts).toBe(2);
    });

    test("a holder whose lease lapsed can no longer ack, extend or release", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      const stale = await q.claim({ leaseMs: 1000 });
      clock.t += 1000;
      const current = await q.claim({ leaseMs: 1000 });
      expect(await q.ack(stale!)).toBe(false);
      expect(await q.extend(stale!, 1000)).toBe(false);
      expect(await q.release(stale!)).toBe(false);
      expect(await q.ack(current!)).toBe(true);
    });

    test("a lapsed lease nobody re-claimed cannot be acked either", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      const c = await q.claim({ leaseMs: 1000 });
      clock.t += 1000;
      expect(await q.ack(c!)).toBe(false);
      expect(await q.pendingTopics()).toEqual(["a"]);
    });

    test("extend keeps the message held past its first lease", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      const c = await q.claim({ leaseMs: 1000 });
      clock.t += 900;
      expect(await q.extend(c!, 1000)).toBe(true);
      clock.t += 900;
      expect(await q.claim({ leaseMs: 1000 })).toBeNull();
      expect(await q.ack(c!)).toBe(true);
    });

    test("release makes the message claimable again at once", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1 });
      const c = await q.claim({ leaseMs: 1000 });
      expect(await q.release(c!)).toBe(true);
      const again = await q.claim({ leaseMs: 1000 });
      expect(again!.message.id).toBe(c!.message.id);
      expect(again!.message.attempts).toBe(2);
    });

    test("pendingTopics lists every topic with a message, in flight or not", async () => {
      const { q } = setup();
      await q.enqueue("b", { n: 1 });
      await q.enqueue("a", { n: 2 });
      await q.claim({ topics: ["a"], leaseMs: 1000 });
      expect(await q.pendingTopics()).toEqual(["a", "b"]);
    });

    // ── observability ──────────────────────────────────────────────────────

    test("topics() counts pending and in-flight per topic", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      clock.t += 5;
      await q.enqueue("a", { n: 2 });
      await q.enqueue("b", { n: 3 });
      await q.claim({ topics: ["a"], leaseMs: 1000 });
      expect(await q.topics()).toEqual([
        { topic: "a", pending: 1, inFlight: 1, oldestEnqueuedAt: 1_000_000 },
        { topic: "b", pending: 1, inFlight: 0, oldestEnqueuedAt: 1_000_005 },
      ]);
    });

    test("list() shows every message with its state and content, never the token", async () => {
      const { q } = setup();
      await q.enqueue("a", { n: 1, note: "first" });
      await q.enqueue("a", { n: 2 });
      const c = await q.claim({ leaseMs: 1000 });
      const views = await q.list("a");
      expect(views.map((v) => [v.payload.n, v.state])).toEqual([[1, "in_flight"], [2, "pending"]]);
      expect(views[0]!.payload.note).toBe("first");
      expect(views[0]!.leaseUntil).toBe(c!.leaseUntil);
      expect(views[0]!.claimedAt).toBe(1_000_000);
      expect(views[1]!.leaseUntil).toBeNull();
      expect(JSON.stringify(views)).not.toContain(c!.token);
    });

    test("list() pages", async () => {
      const { q } = setup();
      for (const n of [1, 2, 3, 4]) await q.enqueue("a", { n });
      expect((await q.list("a", { limit: 2, offset: 1 })).map((v) => v.payload.n)).toEqual([2, 3]);
    });

    test("a lapsed lease reads as pending", async () => {
      const { q, clock } = setup();
      await q.enqueue("a", { n: 1 });
      const c = await q.claim({ leaseMs: 1000 });
      clock.t += 1000;
      const v = await q.get(c!.message.id);
      expect(v!.state).toBe("pending");
      expect(v!.leaseUntil).toBeNull();
    });

    test("get() finds one message by id, and null once it is acked", async () => {
      const { q } = setup();
      const m = await q.enqueue("a", { n: 1 });
      expect((await q.get(m.id))!.payload.n).toBe(1);
      await q.ack((await q.claim({ leaseMs: 1000 }))!);
      expect(await q.get(m.id)).toBeNull();
    });

    test("observing changes nothing a consumer gets", async () => {
      const { q } = setup();
      const m = await q.enqueue("a", { n: 1 });
      await q.topics();
      await q.list("a");
      await q.get(m.id);
      const c = await q.claim({ leaseMs: 1000 });
      expect(c!.message.id).toBe(m.id);
      expect(c!.message.attempts).toBe(1);
    });
  });
}
