// A message queue, independent of what stores it.
//
// Topics hold messages in arrival order. A consumer claims the oldest available
// message for a while (a lease), processes it, and acks it; an ack removes it.
// A lease that runs out without an ack puts the message back, so a consumer
// that dies mid-message loses nothing.
//
// Within a topic, one message is in flight at a time: a topic whose head is
// claimed is skipped until that message is acked, released or its lease lapses.
// Across topics, how many run at once is the worker's concurrency.
//
// Every call is async so a backend that talks over a wire (Redis, Postgres) fits
// the same shape as the SQLite one.

export type Message<P> = {
  id: string;
  topic: string;
  payload: P;
  enqueuedAt: number;
  /** How many times this message has been claimed, this claim included. */
  attempts: number;
};

/** A message held by one consumer. The token is what proves it is still theirs. */
export type Claim<P> = {
  message: Message<P>;
  token: string;
  leaseUntil: number;
};

export interface Queue<P> {
  /** Add a message to the end of `topic`. */
  enqueue(topic: string, payload: P): Promise<Message<P>>;

  /** The messages a consumer could claim next in `topic`, oldest first. Claims nothing. */
  peek(topic: string, limit?: number): Promise<Message<P>[]>;

  /** Topics with at least one message waiting, in flight or not. */
  pendingTopics(): Promise<string[]>;

  /**
   * Take the oldest claimable message — across `topics` when given, across every
   * topic otherwise — and hold it for `leaseMs`. Null when nothing is claimable.
   */
  claim(opts: { topics?: string[]; leaseMs: number }): Promise<Claim<P> | null>;

  /** Push the lease out to `leaseMs` from now. False when the claim is no longer held. */
  extend(claim: Claim<P>, leaseMs: number): Promise<boolean>;

  /** Processed: the message is removed. False when the claim is no longer held. */
  ack(claim: Claim<P>): Promise<boolean>;

  /** Not processed: the message is claimable again now. False when the claim is no longer held. */
  release(claim: Claim<P>): Promise<boolean>;
}

/** What an observer sees of a message: everything but the lease token. */
export type MessageView<P> = Message<P> & {
  state: "pending" | "in_flight";
  claimedAt: number | null;
  leaseUntil: number | null;
};

export type TopicStats = {
  topic: string;
  pending: number;
  inFlight: number;
  oldestEnqueuedAt: number;
};

/**
 * Read-only view for someone outside the queue — a dashboard, an operator.
 * Nothing here claims, counts as an attempt or changes what a consumer gets.
 */
export interface QueueInspector<P> {
  topics(): Promise<TopicStats[]>;
  /** Every message in `topic`, in flight or not, oldest first. */
  list(topic: string, opts?: { limit?: number; offset?: number }): Promise<MessageView<P>[]>;
  get(id: string): Promise<MessageView<P> | null>;
}
