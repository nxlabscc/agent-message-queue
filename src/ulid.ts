// Tiny ULID generator. Sortable, 26-char Crockford base-32.
// Spec: https://github.com/ulid/spec
// Two ids of the same millisecond are in random order; the SQLite queue orders
// by its own sequence.

const ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const ENCODING_LEN = ENCODING.length;
const TIME_LEN = 10;
const RAND_LEN = 16;

function encodeTime(now: number): string {
  let out = "";
  let n = now;
  for (let i = TIME_LEN - 1; i >= 0; i--) {
    out = ENCODING[n % ENCODING_LEN] + out;
    n = Math.floor(n / ENCODING_LEN);
  }
  return out;
}

function encodeRandom(): string {
  let out = "";
  const bytes = new Uint8Array(RAND_LEN);
  crypto.getRandomValues(bytes);
  for (let i = 0; i < RAND_LEN; i++) {
    out += ENCODING[bytes[i]! % ENCODING_LEN];
  }
  return out;
}

export function ulid(now: number = Date.now()): string {
  return encodeTime(now) + encodeRandom();
}
