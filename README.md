# LinkPlease Assignment — Complete Step-by-Step Guide

Everything you need to do, in order. Follow it top to bottom.

---

## STEP 0 — Understand what you built (2 minutes)

One Python file (`app.py`) that:

- **Receives** comment events at `POST /webhook` and answers instantly (the real work happens in a background thread — this is why we don't time out).
- **Stores rules** created at `POST /rules` ("if a comment contains PRICE, DM this message").
- **Sends DMs** through the mock API, retrying on rate limits (429) and random failures (500), never sending the same user the same rule's DM twice.
- **Double-checks delivery** — the API saying "accepted" doesn't mean "delivered", so a second thread polls each DM until it's confirmed, and re-sends failed ones.
- **Reports** honest counters at `GET /stats`.

---

## STEP 1 — Get your API key (5 minutes)

Use your real details — this is how they identify your submission.

**1a. Apply** (replace with YOUR info):

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/apply \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Your Name",
    "email": "you@example.com",
    "phone": "+91XXXXXXXXXX",
    "linkedin_url": "https://linkedin.com/in/you"
  }'
```

**1b. Get the key** (same email):

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/keygen \
  -H "Content-Type: application/json" \
  -d '{"email": "you@example.com"}'
```

You'll get back `{"api_key": "...", ...}`. **Save that key somewhere safe.**
If you get a 403, step 1a didn't go through yet — do it again.

---

## STEP 2 — Run it locally (5 minutes)

You need Python 3.10+ installed.

```bash
cd linkplease-assignment
pip install -r requirements.txt

# Windows (PowerShell):
$env:PSEUDOGRAM_API_KEY = "your-key-here"
# Mac/Linux:
export PSEUDOGRAM_API_KEY="your-key-here"

python app.py
```

You should see Flask start on port 8000. Leave this terminal running.

---

## STEP 3 — Test each endpoint by hand (10 minutes)

Open a **second terminal**.

**3a. Create a rule:**

```bash
curl -X POST http://localhost:8000/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword": "PRICE", "dm_message": "Here is the price list: ..."}'
```

Expect: `201` with a `rule_id`.

**3b. Check stats (should be all zeros):**

```bash
curl http://localhost:8000/stats
```

**3c. Send a fake comment to your own webhook:**

Signature checking is skipped when testing without a key set, but since you set
the key, you need a valid signature. Easiest way — use this tiny Python helper:

```bash
python - <<'EOF'
import hmac, hashlib, json, os, requests
key = os.environ["PSEUDOGRAM_API_KEY"].encode()
body = json.dumps({
  "event_id": "evt_test_1", "event_type": "comment.created",
  "sent_at": "2026-08-15T10:00:00Z",
  "data": {"comment_id": "cmt_test_1", "post_id": "post_1",
           "text": "PRICE please!", "created_at": "2026-08-15T10:00:00Z",
           "from": {"user_id": "usr_test_1", "username": "tester"}}
}).encode()
sig = "sha256=" + hmac.new(key, body, hashlib.sha256).hexdigest()
r = requests.post("http://localhost:8000/webhook", data=body,
    headers={"Content-Type": "application/json", "X-PseudoGram-Signature": sig})
print(r.status_code, r.text)
EOF
```

Then check `/stats` again after ~15 seconds — you should see `queued: 1`, and
once the reconciler confirms delivery, `sent: 1`.

Send the **same event again** — `stats` should NOT change (duplicate event_id skipped).
Send a **new event_id but same user + same keyword** — `duplicates_blocked` should go up by 1.

---

## STEP 4 — Deploy (30 minutes)

The graders hit a public URL, so localhost isn't enough. Render's free tier works:

1. Push this folder to a **public GitHub repo** (must contain FAILURES.md in the root).
2. Go to render.com → New → Web Service → connect the repo.
3. Settings:
   - **Build command:** `pip install -r requirements.txt`
   - **Start command:** `gunicorn -w 1 --threads 8 app:app`
     (`-w 1` = exactly ONE process. Important! Our state is in memory —
     two processes would have two separate copies of everything.)
   - **Environment variable:** `PSEUDOGRAM_API_KEY` = your key
4. Deploy. Your URL looks like `https://your-app.onrender.com`.
5. **Free tier gotcha:** Render sleeps after 15 min idle. Use a free uptime
   pinger (e.g., UptimeRobot hitting `/` every 10 min) so the graders never
   hit a sleeping app. A dead link = automatic zero.

Test the deployed URL: `curl https://your-app.onrender.com/stats`

---

## STEP 5 — Run the real simulation (repeat until clean)

Recreate your rule on the **deployed** app first (memory was empty on deploy):

```bash
curl -X POST https://your-app.onrender.com/rules \
  -H "Content-Type: application/json" \
  -d '{"keyword": "PRICE", "dm_message": "Here is the price list: ..."}'
```

Start small (50 events), then work up to the full 500:

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/simulate/start \
  -H "Content-Type: application/json" -H "X-API-Key: your-key-here" \
  -d '{"webhook_url": "https://your-app.onrender.com/webhook", "count": 50, "duration_seconds": 10}'
```

Save the `run_id` it returns. Wait for the queue to drain (watch `/stats`
until `queued` reaches 0 — with the 10-per-60s rate limit, 500 events can
take several minutes). Then compare:

```bash
curl https://pseudogram-api.onrender.com/v1/simulate/RUN_ID/truth \
  -H "X-API-Key: your-key-here"
```

Check: every matching user got exactly one DM, duplicates were blocked,
nothing lost. Anything off → fix, redeploy, re-run. Note real observations
for FAILURES.md while you do this.

---

## STEP 6 — FAILURES.md, Loom, submit

1. **FAILURES.md** — a starter is included. Read it, verify each point is true
   of YOUR final code, add anything you observed during simulations, delete
   the HTML comment at the bottom.
2. **Loom** (3 min, screen + voice): answer exactly two things —
   one tradeoff you made (e.g., "single worker thread: no race conditions,
   but slower throughput"), and what you'd do differently with a week
   (e.g., "put state in SQLite/Redis so restarts don't lose DMs").
3. **Submit:**

```bash
curl -X POST https://pseudogram-api.onrender.com/v1/submit \
  -H "Content-Type: application/json" \
  -d '{
    "email": "you@example.com",
    "github_repo": "https://github.com/you/repo",
    "working_url": "https://your-app.onrender.com",
    "loom_url": "https://loom.com/share/...",
    "parts_completed": "A+B+C",
    "start_date": "2026-08-15"
  }'
```

You can submit early and resubmit later — the last one counts.
**Deadline: Aug 17, 11:59 PM IST. Keep the app live for 7 days after.**

---

## How the code maps to the grading

| Assignment requirement | Where in app.py |
|---|---|
| /webhook returns 200 in <5s | `webhook()` — just queues and returns |
| Rules with case-insensitive matching | `create_rule()` + `process_event()` step 4 |
| Never DM same user twice per rule | `dmed_pairs` set, step 5 |
| No DM silently lost on API failure | retries in `send_dm()` + reconciler |
| Signature verification (Part B) | `signature_is_valid()` |
| Accurate /stats under load (Part B) | every counter change holds `lock` |
| Reconcile delivery status (Part C) | `reconciler_loop()` |
| comment.deleted handling (Part C) | `deleted_comments` set, steps 2–3 |
| Rate limit never breached (Part C) | `wait_for_rate_limit_slot()` |
