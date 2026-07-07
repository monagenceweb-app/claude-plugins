// Shared helpers for the events plugin. Pure Node (>=18) — no python,
// curl, jq or git: the only runtime dependency is `node`, which Claude
// Code already requires to run. Node 18+ ships global `fetch`.
import {
  appendFileSync,
  chmodSync,
  closeSync,
  existsSync,
  fstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readSync,
  rmSync,
  statSync,
  writeFileSync,
} from 'node:fs'
import { spawn } from 'node:child_process'
import { homedir, hostname } from 'node:os'
import { basename, dirname, join } from 'node:path'

export const HOOKS_DIR = join(homedir(), '.claude', 'hooks')
export const TOKEN_FILE = join(HOOKS_DIR, '.monagence-token')
export const PENDING_FILE = join(HOOKS_DIR, '.monagence-pending.json')
export const LOG_FILE = join(HOOKS_DIR, 'monagence-events.log')
/** Durable local queue: events are appended here instantly and flushed async. */
export const SPOOL_FILE = join(HOOKS_DIR, 'monagence-spool.ndjson')
/** Directory used as an atomic cross-process lock for the flusher. */
export const LOCK_DIR = join(HOOKS_DIR, '.monagence-flush.lock')

export const BASE_URL = (process.env.MONAGENCE_BASE_URL || 'https://app.monagence.pro').replace(
  /\/+$/,
  '',
)
export const INGEST_URL = process.env.MONAGENCE_ENDPOINT || `${BASE_URL}/api/claude/events`

function ensureDir() {
  try {
    mkdirSync(HOOKS_DIR, { recursive: true })
  } catch {
    /* ignore */
  }
}

export function readToken() {
  if (process.env.MONAGENCE_TOKEN) return process.env.MONAGENCE_TOKEN.trim() || null
  try {
    return readFileSync(TOKEN_FILE, 'utf8').trim() || null
  } catch {
    return null
  }
}

export function writeToken(token) {
  ensureDir()
  writeFileSync(TOKEN_FILE, token, { mode: 0o600 })
  try {
    chmodSync(TOKEN_FILE, 0o600)
  } catch {
    /* Windows: mode bits not enforced — fine */
  }
}

/** Remove the stored token — used when the server rejects it (revoked/invalid)
 *  so SessionStart re-triggers the device flow and the link self-heals. */
export function clearToken() {
  try {
    rmSync(TOKEN_FILE)
  } catch {
    /* ignore */
  }
}

export function readPending() {
  try {
    const p = JSON.parse(readFileSync(PENDING_FILE, 'utf8'))
    if (p && p.deviceCode && p.expiresAt) return p
  } catch {
    /* ignore */
  }
  return null
}

export function writePending(p) {
  ensureDir()
  writeFileSync(PENDING_FILE, JSON.stringify(p), { mode: 0o600 })
  try {
    chmodSync(PENDING_FILE, 0o600)
  } catch {
    /* ignore */
  }
}

export function clearPending() {
  try {
    rmSync(PENDING_FILE)
  } catch {
    /* ignore */
  }
}

export async function deviceStart(label) {
  const r = await fetch(`${BASE_URL}/api/claude/device/start`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ label }),
  })
  if (!r.ok) throw new Error(`device/start ${r.status}`)
  return r.json()
}

export async function devicePoll(deviceCode) {
  const r = await fetch(`${BASE_URL}/api/claude/device/poll`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ deviceCode }),
  })
  if (!r.ok) throw new Error(`device/poll ${r.status}`)
  return r.json()
}

/** Nearest ancestor dir containing `.git`, else the cwd basename. */
export function projectSlug(cwd) {
  let dir = cwd
  for (let i = 0; i < 40; i++) {
    if (existsSync(join(dir, '.git'))) return basename(dir)
    const parent = dirname(dir)
    if (parent === dir) break
    dir = parent
  }
  return basename(cwd)
}

export function deviceLabel() {
  try {
    return hostname() || 'claude-code'
  } catch {
    return 'claude-code'
  }
}

/** Read all of stdin (the hook JSON). Resolves '' when there's no pipe. */
export function readStdin() {
  return new Promise((resolve) => {
    if (process.stdin.isTTY) return resolve('')
    let data = ''
    process.stdin.setEncoding('utf8')
    process.stdin.on('data', (c) => (data += c))
    process.stdin.on('end', () => resolve(data))
    process.stdin.on('error', () => resolve(data))
  })
}

// ──────────────────────────────────────────────────────────────────────
// Durable spool + async flush — why this design is "infallible":
//   • the HOOK never does network I/O and never parses a whole transcript,
//     so it can't be slow enough to time out or get disabled;
//   • every event is appended to a local file BEFORE anything else, and is
//     removed only after the server acknowledges it, so a crash, an offline
//     machine or a server outage can never lose data — it retries later.
// ──────────────────────────────────────────────────────────────────────

/** Append one event to the local queue. Atomic single-line write. */
export function spool(record) {
  ensureDir()
  try {
    appendFileSync(SPOOL_FILE, JSON.stringify(record) + '\n')
  } catch {
    /* ignore — worst case this one event isn't queued */
  }
}

export function readSpoolLines() {
  try {
    return readFileSync(SPOOL_FILE, 'utf8').split('\n').filter((l) => l.trim())
  } catch {
    return []
  }
}

export function writeSpoolLines(lines) {
  ensureDir()
  try {
    writeFileSync(SPOOL_FILE, lines.length ? lines.join('\n') + '\n' : '')
  } catch {
    /* ignore */
  }
}

/**
 * Read only the TAIL of a transcript (default 512 KB) and return the parsed
 * JSONL entries. The data we need (last assistant message, last tool_result)
 * is always at the end, so this bounds the hook's cost regardless of how large
 * the session transcript has grown.
 */
export function readTranscriptTail(path, maxBytes = 512 * 1024) {
  if (!path) return []
  let fd
  try {
    fd = openSync(path, 'r')
    const size = fstatSync(fd).size
    const start = size > maxBytes ? size - maxBytes : 0
    const len = size - start
    const buf = Buffer.allocUnsafe(len)
    readSync(fd, buf, 0, len, start)
    let text = buf.toString('utf8')
    // Drop the first (probably partial) line when we didn't start at 0.
    if (start > 0) {
      const nl = text.indexOf('\n')
      if (nl >= 0) text = text.slice(nl + 1)
    }
    const out = []
    for (const line of text.split('\n')) {
      const s = line.trim()
      if (!s) continue
      try {
        out.push(JSON.parse(s))
      } catch {
        /* skip malformed line */
      }
    }
    return out
  } catch {
    return []
  } finally {
    if (fd !== undefined) {
      try {
        closeSync(fd)
      } catch {
        /* ignore */
      }
    }
  }
}

/** Atomic lock via mkdir; steals a stale lock (>2 min) left by a dead flusher. */
export function acquireFlushLock() {
  ensureDir()
  try {
    mkdirSync(LOCK_DIR)
    return true
  } catch {
    try {
      if (Date.now() - statSync(LOCK_DIR).mtimeMs > 120_000) {
        rmSync(LOCK_DIR, { recursive: true, force: true })
        mkdirSync(LOCK_DIR)
        return true
      }
    } catch {
      /* ignore */
    }
    return false
  }
}

export function releaseFlushLock() {
  try {
    rmSync(LOCK_DIR, { recursive: true, force: true })
  } catch {
    /* ignore */
  }
}

/**
 * Spawn the flusher as a DETACHED background process and return immediately.
 * It outlives this hook (and even Claude Code itself), so the hook never waits
 * on the network. Concurrent spawns are harmless — the lock serialises them.
 */
export function triggerFlush(flushScriptPath) {
  try {
    const child = spawn(process.execPath, [flushScriptPath], {
      detached: true,
      stdio: 'ignore',
      windowsHide: true,
    })
    child.unref()
  } catch {
    /* ignore — the next SessionStart/Stop will try again */
  }
}

// ── Event body assembly (shared) ──────────────────────────────────────
function normPath(p) {
  if (!p) return null
  const m = /^\/([a-zA-Z])\/(.*)$/.exec(p) // /c/Users/… → C:/Users/…
  return m ? `${m[1].toUpperCase()}:/${m[2]}` : p
}
const tsOf = (s) => {
  const t = Date.parse(s || '')
  return Number.isNaN(t) ? null : t
}

/** Path to the transcript, normalised for the current OS (or null). */
export function transcriptPathOf(pl) {
  return normPath(pl?.transcript_path)
}

/**
 * Build the ingest body for one hook event from the payload + (tail) transcript
 * entries. Kept identical to the previous inline logic so analytics are
 * unchanged; only WHERE it runs moved (bounded tail, off the network path).
 */
export function buildEventBody(eventType, pl, entries) {
  const cwd = pl.cwd || process.cwd()
  const body = {
    eventType,
    sessionId: pl.session_id || null,
    toolName: pl.tool_name || null,
    cwd,
    projectSlug: projectSlug(cwd),
  }

  if (eventType === 'UserPromptSubmit') {
    const p = (pl.prompt || pl.user_prompt || '').slice(0, 8000)
    if (p) body.prompt = p
  } else if (eventType === 'Stop') {
    const lastAssistant = [...entries].reverse().find((e) => e?.type === 'assistant')
    const msg = lastAssistant?.message
    if (msg) {
      const u = msg.usage || {}
      if (msg.model) body.model = msg.model
      if (u.input_tokens != null) body.inputTokens = u.input_tokens
      if (u.output_tokens != null) body.outputTokens = u.output_tokens
      if (u.cache_creation_input_tokens != null) body.cacheCreationTokens = u.cache_creation_input_tokens
      if (u.cache_read_input_tokens != null) body.cacheReadTokens = u.cache_read_input_tokens
    }
  } else if (eventType === 'PostToolUse') {
    let resultId = null
    let resultTs = null
    for (let i = entries.length - 1; i >= 0; i--) {
      const content = entries[i]?.message?.content
      if (!Array.isArray(content)) continue
      const tr = content.find((c) => c && c.type === 'tool_result')
      if (tr) {
        resultId = tr.tool_use_id
        resultTs = tsOf(entries[i].timestamp)
        break
      }
    }
    if (resultId) {
      body.toolUseId = resultId
      let useTs = null
      for (const e of entries) {
        const content = e?.message?.content
        if (!Array.isArray(content)) continue
        const tu = content.find((c) => c && c.type === 'tool_use' && c.id === resultId)
        if (tu) {
          useTs = tsOf(e.timestamp)
          break
        }
      }
      if (useTs && resultTs && resultTs >= useTs) body.durationMs = resultTs - useTs
    }
  }

  for (const k of Object.keys(body)) if (body[k] == null) delete body[k]
  return body
}
