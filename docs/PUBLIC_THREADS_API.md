# Public Threads posts API

A public, read-only feed of everything this account posts to Threads --
built specifically so [CumDev](https://cumdev.onrender.com)'s blog can
stream it, without CumDev needing its own separate Threads OAuth
connection. Lives on the **existing** `bettor-dashboard` Render service
(no new service was created for this) alongside its other `/api/*` routes.

**Base URL:** `https://bettor-dashboard-18ni.onrender.com`

Every bot in this codebase (Kalshi perps, Alpaca stocks/crypto/options)
posts to Threads as the **same** account/token, so this one endpoint on
this one service already sees everything, regardless of which bot
actually posted it.

## `GET /api/threads/posts`

Public, unauthenticated. Returns this account's Threads posts, newest
first, served from a durable archive on this side (not just whatever
shallow recent window Meta's own API happens to show right now).

**Query params** (both optional):

| Param | Default | Notes |
|---|---|---|
| `limit` | `25` | Capped at `50`. |
| `since_id` | none | A post `id` from a previous response -- returns only posts newer than it. Use this for incremental polling instead of re-fetching everything every time. |

**Response** (`200`, always -- this route never raises; check `ok`):

```json
{
  "ok": true,
  "count": 2,
  "posts": [
    {
      "id": "17997510071809274",
      "media_type": "IMAGE",
      "media_url": "https://scontent...jpg",
      "permalink": "https://www.threads.com/@elrugger/post/Dch3MrRG8Y8",
      "text": "Bitcoin ETFs just pulled in a record $2.1B this week...\n#Crypto #Bitcoin",
      "timestamp": "2026-08-27T03:51:05+0000",
      "shortcode": "Dch3MrRG8Y8",
      "is_quote_post": false
    },
    {
      "id": "18080195306328756",
      "media_type": "TEXT_POST",
      "permalink": "https://www.threads.com/@elrugger/post/Dch2msMm3jx",
      "text": "Money Bot has restarted!\n\nhttps://cumdev.onrender.com",
      "timestamp": "2026-08-27T03:45:54+0000",
      "shortcode": "Dch2msMm3jx",
      "is_quote_post": false
    }
  ]
}
```

`media_type` is one of `TEXT_POST`, `IMAGE`, `VIDEO`, `CAROUSEL_ALBUM`.
`media_url` is only present on image/video posts.

On failure (upstream Threads API down, token issue, etc.) you still get a
`200` with `{"ok": false, "error": "threads_posts_unavailable", "count": 0, "posts": []}`
-- treat that as "try again later," not a hard error.

CORS is wide open (`Access-Control-Allow-Origin: *`) -- this only ever
re-serves the account's own already-public Threads content, so a
browser-side `fetch` from `cumdev.onrender.com` (or anywhere else) works
directly, no proxy needed.

### Example

```bash
curl "https://bettor-dashboard-18ni.onrender.com/api/threads/posts?limit=10"
```

```ts
// CumDev-side fetch (server component, API route, or client -- CORS is open either way)
const res = await fetch("https://bettor-dashboard-18ni.onrender.com/api/threads/posts?limit=25");
const { ok, posts } = await res.json();
```

For incremental polling once you've stored the newest post id you've
already seen:

```ts
const res = await fetch(
  `https://bettor-dashboard-18ni.onrender.com/api/threads/posts?since_id=${lastSeenId}`
);
```

## `POST /api/threads/posts/sync` -- keeping the archive fresh

`GET /api/threads/posts` above is a public, read-only view. Something
still has to keep the underlying archive up to date, since Meta's own API
only ever shows a shallow recent window. That's this route -- it fetches
the account's current posts from Meta and merges any new ones into the
durable archive.

**This is not meant to be called by CumDev.** It's a trigger route for an
external scheduler (see the cron-job.org setup below). GET and POST are
both accepted (a plain scheduled HTTP hit is usually a GET).

**Requires authorization** -- same `CRON_SECRET` bearer token every other
scheduled-trigger route on this service already uses:

```text
Authorization: Bearer <CRON_SECRET>
```

Without it: `401 {"ok": false, "error": "Unauthorized"}`. This is the
same secret already configured for this service's other scheduled jobs
(perps tick/collect/train/etc.) -- ask whoever set those up for the value,
it does not need to be a new one.

**Response** (`200`):

```json
{"ok": true, "new_posts": 2, "total_archived": 187}
```

### cron-job.org setup

Create one new job at [console.cron-job.org/jobs](https://console.cron-job.org/jobs):

- **URL:** `https://bettor-dashboard-18ni.onrender.com/api/threads/posts/sync`
- **Method:** `GET` (or `POST` -- both work)
- **Headers:** `Authorization: Bearer <CRON_SECRET>` (the real secret value, not this placeholder)
- **Schedule:** every 1 minute, to match the ~1-minute end-to-end freshness the CumDev blog side is targeting (its own cron interval + shortened page cache window)

Why 1 minute is safe here: Meta's own list is capped to the 50 most
recent posts per fetch -- if this account posted more than 50 times
between two syncs, the ones in between would be skipped, but at this
account's actual posting cadence (trending news every 30 min, hourly
status, occasional trade events) that's nowhere close, so a 1-minute
interval leaves a huge safety margin, not a tight one. It also has a
side benefit: Render's free/starter tier spins this service down after a
period of idle, and the first request after that takes ~20-30s to wake it
back up (confirmed live) -- a 1-minute cron keeps it warm, so CumDev's own
polls of `/api/threads/posts` hit a warm service instead of eating that
cold-start delay themselves.

## Why this design (not a raw push stream)

"Streaming" here means CumDev polls a fast, cheap, always-fresh endpoint
-- not a held-open connection (SSE/WebSocket). The bettor-dashboard
service runs a single gunicorn worker/thread; holding one long-lived
streaming connection open would block every other request that service
needs to handle (its own dashboard, manual trigger routes, health
checks). Polling `/api/threads/posts` every minute or so from CumDev's
side gets the same practical result -- new posts show up within one poll
cycle -- without that risk. This mirrors how CumDev's own existing
Threads-to-blog sync (via its own cron-job.org job hitting its `/api/threads/sync`
route) already works.
