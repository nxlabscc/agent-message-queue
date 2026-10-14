import type { Message, Queue, QueueInspector } from "./types";
import { createWorker, type Worker } from "./worker";

// Messages that start agent sessions.
//
// Each topic gets one worker, and a topic runs one message at a time, oldest
// first. For each message the worker finds its agent, waits until that agent
// has no session open, and starts one with `startSession`. It acks the message
// once that session has ended.
//
// An agent never has two sessions open. Inside this process, every worker of
// one `createAgentMessaging` holds the agent while its session runs, so two
// topics wait for each other on the same agent. Sessions opened elsewhere
// answer through `isBusy`; a start that loses the race to one of them returns
// null and the worker waits again.
//
// What happens to a message whose session failed: it is consumed, and
// `onError` hears about it. A stop while the worker waits on a busy agent puts
// the message back, untouched.

/** What a worker is doing with the message it holds. */
export type AgentPhase = "taken" | "blocked" | "running" | "closing";

/** The session ran (`errored` says how it ended), or null: the agent turned
 *  busy before it could start. */
export type SessionOutcome = { errored: boolean } | null;

export type AgentMessagingOptions<P> = {
  queue: Queue<P> & QueueInspector<P>;
  /** The agent a message is for. */
  agentOf: (payload: P, topic: string) => string;
  /** True while the agent has a session open that this process did not start. */
  isBusy: (agent: string) => boolean | Promise<boolean>;
  /** Open the agent's session for the message and resolve when it has ended. */
  startSession: (message: Message<P>) => Promise<SessionOutcome>;
  leaseMs: number;
  /** How long an idle topic waits before trying to claim again. */
  pollMs: number;
  /** How long a message waits before asking again whether its agent is free. */
  busyPollMs: number;
  /** How long to hold off before claiming, in ms; 0 = claim. */
  gate?: () => number;
  /** Told about every message whose session errored or threw. */
  onError?: (err: unknown, message: Message<P>) => void;
};

export type AgentMessageView<P> = {
  id: string;
  topic: string;
  agent: string;
  /** `waiting` in its topic's line, or what its worker is doing with it. A
   *  message in flight with no phase here is held by another process: `taken`. */
  state: "waiting" | AgentPhase;
  /** Its place in its topic's line while waiting, from 1; null once taken. */
  position: number | null;
  enqueuedAt: number;
  claimedAt: number | null;
  attempts: number;
  payload: P;
};

export type AgentMessaging<P> = {
  /** Queue a message. With `key`, a message whose key is already queued in the
   *  topic, waiting or in flight, is refused with `AlreadyQueued`. */
  send(topic: string, payload: P, opts?: { key?: (payload: P) => string }): Promise<Message<P>>;
  /** One worker per topic. */
  start(topics: string[]): void;
  /** Stop taking messages; sessions in progress finish, waits on a busy agent
   *  give up and put their message back. */
  stop(): Promise<void>;
  /** Every message of `topics`, topic by topic, oldest first. */
  view(topics: string[], opts?: { limit?: number }): Promise<AgentMessageView<P>[]>;
  phaseOf(messageId: string): AgentPhase | null;
};

/** A send refused: a message with the same key is already queued. */
export class AlreadyQueued extends Error {
  constructor(readonly key: string, readonly topic: string) {
    super(`${key} is already queued in ${topic}`);
  }
}

/** Raised inside a worker that stops while it waits: its message goes back. */
export class Stopping extends Error {}

/** Queue `payload` on `topic`; with `key`, refuse one already queued there. */
export async function sendAgentMessage<P>(
  queue: Queue<P> & QueueInspector<P>,
  topic: string,
  payload: P,
  opts: { key?: (payload: P) => string } = {},
): Promise<Message<P>> {
  if (opts.key) {
    const key = opts.key(payload);
    const page = 500;
    for (let offset = 0; ; offset += page) {
      const queued = await queue.list(topic, { limit: page, offset });
      if (queued.some((m) => opts.key!(m.payload) === key)) throw new AlreadyQueued(key, topic);
      if (queued.length < page) break;
    }
  }
  return queue.enqueue(topic, payload);
}

export function createAgentMessaging<P>(opts: AgentMessagingOptions<P>): AgentMessaging<P> {
  const phases = new Map<string, AgentPhase>();
  const held = new Set<string>();             // agents with a session this process started
  const sleepers = new Set<() => void>();
  let workers: Worker[] = [];
  let stopping = false;

  const sleep = (ms: number) =>
    new Promise<void>((resolve) => {
      const wake = () => {
        clearTimeout(timer);
        sleepers.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, ms);
      sleepers.add(wake);
    });

  async function handle(message: Message<P>): Promise<void> {
    const agent = opts.agentOf(message.payload, message.topic);
    for (;;) {
      // Wait until the agent is free: here, then elsewhere, then here again in
      // the same tick as taking it, so no other worker slips in between.
      while (held.has(agent) || await opts.isBusy(agent) || held.has(agent)) {
        phases.set(message.id, "blocked");
        if (stopping) throw new Stopping(`${message.id}: stopping while ${agent} is busy`);
        await sleep(opts.busyPollMs);
      }
      if (stopping) throw new Stopping(`${message.id}: stopping before ${agent}'s session`);

      held.add(agent);
      phases.set(message.id, "running");
      let outcome: SessionOutcome;
      try {
        outcome = await opts.startSession(message);
      } finally {
        held.delete(agent);
      }
      if (outcome === null) continue;          // the agent got busy in between
      if (outcome.errored) throw new Error(`${message.id}: ${agent}'s session errored`);
      return;
    }
  }

  return {
    send: (topic, payload, sendOpts) => sendAgentMessage(opts.queue, topic, payload, sendOpts),

    start(topics) {
      if (workers.length > 0) return;
      stopping = false;
      workers = topics.map((topic) =>
        createWorker(opts.queue, {
          concurrency: 1,
          leaseMs: opts.leaseMs,
          pollMs: opts.pollMs,
          topics: () => [topic],
          gate: opts.gate,
          handler: handle,
          onClaim: (m) => phases.set(m.id, "taken"),
          onHandled: (m) => phases.set(m.id, "closing"),
          onDone: (m) => phases.delete(m.id),
          onFailure: (err) => (err instanceof Stopping ? "release" : "ack"),
          onError: (err, m) => {
            if (!(err instanceof Stopping)) opts.onError?.(err, m);
          },
        }),
      );
      for (const w of workers) w.start();
    },

    async stop() {
      stopping = true;
      for (const wake of [...sleepers]) wake();
      await Promise.all(workers.map((w) => w.stop()));
      workers = [];
    },

    async view(topics, viewOpts = {}) {
      const out: AgentMessageView<P>[] = [];
      for (const topic of topics) {
        let position = 0;
        for (const m of await opts.queue.list(topic, { limit: viewOpts.limit ?? 500 })) {
          const waiting = m.state === "pending";
          out.push({
            id: m.id,
            topic,
            agent: opts.agentOf(m.payload, topic),
            state: waiting ? "waiting" : (phases.get(m.id) ?? "taken"),
            position: waiting ? ++position : null,
            enqueuedAt: m.enqueuedAt,
            claimedAt: m.claimedAt,
            attempts: m.attempts,
            payload: m.payload,
          });
        }
      }
      return out;
    },

    phaseOf: (id) => phases.get(id) ?? null,
  };
}
