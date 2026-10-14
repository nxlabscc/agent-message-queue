# @nxlabs/agent-message-queue

A topic message queue for Bun, in TypeScript source.

- **Topics** hold messages in arrival order.
- A consumer **claims** the oldest available message for a lease, handles it,
  and **acks** it; an ack removes it. A lease that runs out puts the message
  back, so a consumer that dies mid-message loses nothing.
- **One message in flight per topic**: a topic whose head is claimed is skipped
  until that message is acked, released or its lease lapses. How many topics
  run at once is the worker's concurrency.
- The `Queue` interface is async and independent of the store. SQLite
  (`bun:sqlite`) is the backend that ships; any other backend proves itself
  against the same contract tests.

## Install

```json
"dependencies": {
  "@nxlabs/agent-message-queue": "github:nxlabscc/agent-message-queue#v0.1.0"
}
```

Bun runs the `.ts` files as they are; there is no build step.

## Use

```ts
import { Database } from "bun:sqlite";
import { createWorker } from "@nxlabs/agent-message-queue";
import { createSqliteQueue } from "@nxlabs/agent-message-queue/sqlite";

type Job = { agent: string; text: string };

const queue = createSqliteQueue<Job>(new Database("queue.db", { create: true }));
await queue.enqueue("agent:ancano", { agent: "ancano", text: "hello" });

const worker = createWorker(queue, {
  concurrency: 4,          // topics handled at once
  leaseMs: 60_000,         // renewed every half lease while the handler runs
  pollMs: 1_000,           // idle wait between claims
  onFailure: "release",    // or "ack", or (err, message) => "ack" | "release"
  handler: async (message) => {
    console.log(message.topic, message.payload, message.attempts);
  },
});
worker.start();
// ...
await worker.stop();       // stops claiming, waits for the messages in hand
```

### Worker options

| Option | |
|---|---|
| `handler(message)` | handles one message; returning acks it, throwing goes to `onFailure` |
| `concurrency` | messages handled at once, across topics (default 1) |
| `leaseMs` | how long a claim holds a message; renewed every half lease while the handler runs |
| `pollMs` | how long an idle loop waits before claiming again |
| `topics()` | the topics to claim from, or every topic when it returns `undefined` |
| `onFailure` | `"ack"` consumes a failed message, `"release"` makes it claimable again at once; a function decides per failure |
| `onError(err, message)` | told about every handler failure |
| `gate()` | ms to hold off before the next claim; 0 claims |
| `onClaim(message)` | a message was claimed; its handler has not started |
| `onHandled(message)` | its handler returned or threw; the ack or release has not happened |
| `onDone(message)` | acked or released; the worker no longer holds it |

### Inspecting

`createSqliteQueue` also returns a `QueueInspector`: `topics()` (pending and
in-flight counts per topic), `list(topic)` and `get(id)`. These read only; they
claim nothing and count no attempt.

## A new backend

Implement `Queue<P> & QueueInspector<P>` and run the contract against it:

```ts
import { queueContract } from "@nxlabs/agent-message-queue/contract";

queueContract("postgres", (now) => createPostgresQueue(pool, { now }));
```

The factory receives the clock the queue must measure leases against, so lease
expiry is tested by moving time.

## Develop

```sh
bun install
bun test
bun run typecheck
```
