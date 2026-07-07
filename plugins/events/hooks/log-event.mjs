#!/usr/bin/env node
// Claude Code event logger → Mon Agence Web.
//
// This hook is deliberately near-instant and NEVER touches the network: it
// reads the payload, enriches it from the TAIL of the transcript (bounded), and
// appends the event to a local durable spool. A detached flusher (flush.mjs)
// sends spooled events with retries and clears them only once the server acks —
// so a slow/offline network can never make this hook slow, time out, get
// disabled, or lose data. See lib.mjs for the rationale.
import { randomUUID } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import {
  buildEventBody,
  readStdin,
  readTranscriptTail,
  spool,
  transcriptPathOf,
  triggerFlush,
} from './lib.mjs'

const eventType = process.argv[2] || 'unknown'

const raw = await readStdin()
let pl = {}
try {
  pl = raw ? JSON.parse(raw) : {}
} catch {
  pl = {}
}
if (!pl || typeof pl !== 'object') pl = {}

// Only Stop / PostToolUse need transcript-derived fields (tokens / duration).
const entries =
  eventType === 'Stop' || eventType === 'PostToolUse'
    ? readTranscriptTail(transcriptPathOf(pl))
    : []

const body = buildEventBody(eventType, pl, entries)
body.eventId = randomUUID() // idempotency key for safe retries
body.occurredAt = new Date().toISOString() // real event time, preserved if flushed later

spool(body)

// Kick a background flush at the end of a turn (Stop). Other events just queue;
// they ride out with the turn's flush, and SessionStart flushes any leftovers.
if (eventType === 'Stop') {
  triggerFlush(fileURLToPath(new URL('./flush.mjs', import.meta.url)))
}

process.exitCode = 0
