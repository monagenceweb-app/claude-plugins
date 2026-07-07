#!/usr/bin/env node
// SessionStart hook: if this machine isn't linked yet, kick off the OAuth
// device flow and surface the approval URL + code to the user (via the
// hook's additionalContext). No blocking, no daemon — once the user
// approves, log-event.mjs redeems the token on the next event.
import { fileURLToPath } from 'node:url'
import { deviceLabel, deviceStart, readPending, readToken, triggerFlush, writePending } from './lib.mjs'

function emit(message) {
  process.stdout.write(
    JSON.stringify({
      hookSpecificOutput: { hookEventName: 'SessionStart', additionalContext: message },
    }),
  )
}

// Always try to drain any events left queued from a previous session (crash,
// offline, server outage). Detached, so it never delays the session start.
triggerFlush(fileURLToPath(new URL('./flush.mjs', import.meta.url)))

// Single emit path. Guards must NOT use `process.exitCode = 0` to "stop" — that
// only sets the exit code, it doesn't halt the script (and `return` is illegal
// at the top level of an ESM module), so the device flow used to re-run and the
// approval message reappeared every session even when already linked. An
// if/else-if/else chain short-circuits correctly and falls through to the end.
if (readToken()) {
  // Already linked — nothing to surface.
} else {
  const pending = readPending()
  if (pending && Date.parse(pending.expiresAt) > Date.now()) {
    emit(
      `Mon Agence Web : enregistrement Claude Code en attente d'autorisation. ` +
        `Ouvre ${pending.url} et valide le code ${pending.userCode} pour l'activer. ` +
        `Préviens l'utilisateur.`,
    )
  } else {
    try {
      const d = await deviceStart(deviceLabel())
      const url = d.verificationUriComplete || d.verificationUri
      writePending({
        deviceCode: d.deviceCode,
        userCode: d.userCode,
        url,
        expiresAt: new Date(Date.now() + (d.expiresIn || 600) * 1000).toISOString(),
      })
      emit(
        `Mon Agence Web — pour activer l'enregistrement des events Claude Code sur cette machine, ` +
          `dis à l'utilisateur d'ouvrir ${url} et de cliquer « Autoriser » (code ${d.userCode}). ` +
          `L'activation est ensuite automatique, rien d'autre à faire.`,
      )
    } catch {
      // endpoint unreachable — stay silent, retry next session
    }
  }
}
process.exitCode = 0
