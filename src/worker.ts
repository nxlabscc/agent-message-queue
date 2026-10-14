import type { Message, Queue } from "./types";

// Consumers for a Queue: claim → handle → ack, in `concurrency` loops.
//
// While a handler runs its lease is renewed every half lease, so a long handler
// (an LLM run takes minutes) keeps its message and a dead process lets it go.
//
// A handler that throws leaves the message to `onFailure`: "ack" consumes it,
// so a message that breaks its handler is not handed out again forever;
// "release" puts it back to be claimed again at once. A function decides per
// failure.

export type WorkerOptions<P> = {
  handler: (message: Message<P>) => Promise<void>;
  /** How many messages are handled at once, across topics. 1 = one at a time. */
  concurrency?: number;
  leaseMs: number;
  /** How long an idle loop waits before trying to claim again. */
  pollMs: number;
  /** Only these topics, or every topic when absent. */
  topics?: () => string[] | undefined;
  onFailure: FailurePolicy | ((err: unknown, message: Message<P>) => FailurePolicy);
  /** Told about every handler failure, whatever `onFailure` then does. */
  onError?: (err: unknown, message: Message<P>) => void;
  /** How long to hold off before claiming, in ms; 0 = claim. Asked before
   *  every claim, so a pause set while a loop sleeps still holds it. */
  gate?: () => number;
  /** A message was claimed; its handler has not started yet. */
  onClaim?: (message: Message<P>) => void;
  /** Its handler returned or threw; the ack or release has not happened yet. */
  onHandled?: (message: Message<P>) => void;
  /** Acked or released: the worker no longer holds it. */
  onDone?: (message: Message<P>) => void;
};

export type FailurePolicy = "ack" | "release";

export type Worker = {
  start(): void;
  /** Stop claiming and wait for the messages in hand to finish. */
  stop(): Promise<void>;
};

export function createWorker<P>(queue: Queue<P>, opts: WorkerOptions<P>): Worker {
  const concurrency = opts.concurrency ?? 1;
  let running = false;
  let loops: Promise<void>[] = [];
  const sleepers = new Set<() => void>();

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

  async function handleOne(): Promise<boolean> {
    const claim = await queue.claim({ topics: opts.topics?.(), leaseMs: opts.leaseMs });
    if (!claim) return false;
    const message = claim.message;
    opts.onClaim?.(message);

    const heartbeat = setInterval(() => {
      void queue.extend(claim, opts.leaseMs);
    }, Math.max(1, Math.floor(opts.leaseMs / 2)));
    try {
      await opts.handler(message);
      opts.onHandled?.(message);
      clearInterval(heartbeat);
      await queue.ack(claim);
    } catch (err) {
      opts.onHandled?.(message);
      clearInterval(heartbeat);
      opts.onError?.(err, message);
      const policy = typeof opts.onFailure === "function"
        ? opts.onFailure(err, message)
        : opts.onFailure;
      if (policy === "ack") await queue.ack(claim);
      else await queue.release(claim);
    } finally {
      opts.onDone?.(message);
    }
    return true;
  }

  async function loop(): Promise<void> {
    while (running) {
      const hold = opts.gate?.() ?? 0;
      if (hold > 0) {
        await sleep(Math.min(hold, opts.pollMs));
        continue;
      }
      let handled = false;
      try {
        handled = await handleOne();
      } catch (err) {
        // The queue itself failed (not a handler): back off and try again.
        console.error("[queue-worker] claim/ack failed", err);
      }
      if (!handled && running) await sleep(opts.pollMs);
    }
  }

  return {
    start() {
      if (running) return;
      running = true;
      loops = Array.from({ length: concurrency }, () => loop());
    },
    async stop() {
      running = false;
      for (const wake of [...sleepers]) wake();
      await Promise.all(loops);
      loops = [];
    },
  };
}
