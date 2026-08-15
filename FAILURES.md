# FAILURES.md

Ways this system can still lose a DM, send a duplicate, or report a wrong number.

1. **All state is in memory — a restart loses everything.** Rules, the
   seen-event set, the already-DMed set, pending DMs, and stats all live in
   Python dicts/sets. If the process restarts (deploy, crash, Render free-tier
   sleep), any DM waiting in the queue or awaiting reconciliation is lost, and
   /stats resets to zero. Worse: the already-DMed set is also gone, so a
   redelivered event after a restart can cause a *duplicate* DM.

2. **A DM can be marked "claimed" but never sent if we crash mid-send.** I add
   the (user_id, rule_id) pair to `dmed_pairs` *before* calling the send API to
   prevent doubles. If the process dies between claiming and sending, that user
   never gets their DM and nothing will ever retry it — the claim blocks it forever.

3. **Retried DMs after a "failed" status use a new idempotency key.** If a
   resend times out on the network but actually succeeded server-side, and my
   retry loop fires again before I learn the real outcome, the new key means
   the API can't dedupe it — a duplicate DM is possible in that window.

4. **`queued` can be briefly wrong under load.** The counter is incremented
   when a DM is claimed and decremented on terminal outcomes. Between the
   moment /stats is read and the moment the reconciler processes results,
   the number reflects in-flight work that may already be delivered or failed
   on the server side — it's eventually consistent, not instantaneously exact.

5. **Single worker thread = throughput ceiling.** All events are processed
   one at a time. This kills race conditions, but with 500 events in 10 seconds
   plus a 10-per-60s send rate limit, the queue backs up; DMs may take several
   minutes to drain. Nothing is lost, but "queued" stays high for a while.

6. **comment.deleted only helps before the DM is sent.** If the delete event
   arrives after we've already dispatched the DM, we do nothing — the recipient
   keeps a DM for a comment that no longer exists.

<!--
NOTE TO SELF (delete before submitting): re-run the 500-event simulation and
compare against /v1/simulate/{run_id}/truth. Add anything new you observe,
with real numbers ("saw this twice in a 500-event run"). Specific > generic.
-->
