#!/usr/bin/env node
// Detached background flusher for the events plugin. Sends spooled Claude Code
// events to Mon Agence Web and removes them from the local queue ONLY after the
// server acknowledges them. Retries are safe: every event carries an `eventId`
// the server dedupes on, and unsent events stay queued for the next flush.
//
// Runs out-of-band (spawned detached from the hooks), so it can take as long as
// the network needs without ever slowing Claude Code.
import { appendFileSync } from 'node:fs'
import {
  INGEST_URL,
  LOG_FILE,
  acquireFlushLock,
  clearPending,
  devicePoll,
  readPending,
  readSpoolLines,
  readToken,
  releaseFlushLock,
  writeSpoolLines,
  writeToken,
} from './lib.mjs'

const BATCH = 200

function log(line) {
  try {
    appendFileSync(LOG_FILE, line + '\n')
  } catch {
    /* ignore */
  }
}

/** Token from disk, else redeem a pending device authorization (network here,
 *  never in the hook). Returns null when the machine isn't linked yet. */
async function resolveToken() {
  const existing = readToken()
  if (existing) return existing
  const pending = readPending()
  if (!pending) return null
  if (Date.parse(pending.expiresAt) < Date.now()) {
    clearPending()
    return null
  }
  try {
    const res = await devicePoll(pending.deviceCode)
    if (res.status === 'approved' && res.token) {
      writeToken(res.token)
      clearPending()
      return res.token
    }
    if (res.status === 'expired' || res.status === 'denied') clearPending()
  } catch {
    /* network hiccup — try again next flush */
  }
  return null
}

async function postBatch(token, events) {
  const res = await fetch(INGEST_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', authorization: `Bearer ${token}` },
    body: JSON.stringify(events),
  })
  return res
}

async function main() {
  if (!acquireFlushLock()) return // another flusher is already running
  try {
    const token = await resolveToken()
    if (!token) return // not linked yet — keep everything queued

    const lines = readSpoolLines()
    if (!lines.length) return

    const events = []
    for (const l of lines) {
      try {
        events.push(JSON.parse(l))
      } catch {
        /* drop an unparseable line so it can't wedge the queue */
      }
    }
    if (!events.length) {
      writeSpoolLines([])
      return
    }

    const sent = new Set()
    for (let i = 0; i < events.length; i += BATCH) {
      const chunk = events.slice(i, i + BATCH)
      try {
        const res = await postBatch(token, chunk)
        if (res.ok) {
          for (const e of chunk) if (e.eventId) sent.add(e.eventId)
          log(`${new Date().toISOString()} FLUSH sent ${chunk.length} HTTP ${res.status}`)
        } else {
          // 401 → token no longer valid: stop, keep queue for a fresh link.
          const body = (await res.text()).slice(0, 200)
          log(`${new Date().toISOString()} FLUSH HTTP ${res.status} — keeping ${chunk.length} queued. ${body}`)
          if (res.status === 401) break
        }
      } catch (e) {
        log(`${new Date().toISOString()} FLUSH ERROR ${String(e).slice(0, 200)} — keeping queued`)
        break // network down — retry the rest next time
      }
    }

    // Re-read the spool (a hook may have appended while we posted) and keep only
    // the events the server did NOT acknowledge. Events without an eventId
    // (shouldn't happen with this version) are kept to avoid silent loss.
    if (sent.size) {
      const remaining = readSpoolLines().filter((l) => {
        try {
          const e = JSON.parse(l)
          return !(e.eventId && sent.has(e.eventId))
        } catch {
          return false
        }
      })
      writeSpoolLines(remaining)
    }
  } finally {
    releaseFlushLock()
  }
}

await main()
process.exitCode = 0
