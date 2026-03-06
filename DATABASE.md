# Database Setup Guide — Benz Bingo

## The Problem with SQLite on Render

The original `app.py` stored data in a local file (`bingo.db`).
On Render's free tier the filesystem is **ephemeral** — it resets every time the service
restarts or redeploys. That means every user, balance, and game history would be wiped.

For a real money game you need a **persistent, hosted database**.

---

## Free Database Options (comparison)

| Provider | Free tier | Storage | Connections | Expires? |
|---|---|---|---|---|
| **Render PostgreSQL** ✅ | Free | 1 GB | 97 | Never (as of 2024) |
| **Supabase** ✅ | Free | 500 MB | 60 | Pauses after 1 week inactive |
| **Railway** | $5 credit/mo | Unlimited | Unlimited | Uses credit |
| **PlanetScale** | Free (MySQL) | 5 GB | Unlimited | No |

**Recommendation: Render PostgreSQL** — same platform, zero config, never pauses.

---

## Option A — Render PostgreSQL (Recommended)

### Step 1 — Create the database on Render

1. Go to your **Render Dashboard** → click **New +** → **PostgreSQL**
2. Fill in:

   | Field | Value |
   |---|---|
   | **Name** | `benz-bingo-db` |
   | **Region** | Same as your web service (Frankfurt) |
   | **PostgreSQL Version** | `16` |
   | **Instance Type** | `Free` |

3. Click **Create Database**
4. Wait ~1 minute for it to provision
5. On the database info page, find **"Internal Database URL"** — it looks like:
   ```
   postgresql://benz_user:abc123xyz@dpg-xxxxx-a.frankfurt-postgres.render.com/benz_bingo_db
   ```
   **Copy this entire string.**

### Step 2 — Add DATABASE_URL to your Flask web service

1. In Render → go to your **`benz-bingo-api`** web service
2. Click **Environment** tab
3. Click **Add Environment Variable**:

   | Key | Value |
   |---|---|
   | `DATABASE_URL` | *(paste the Internal Database URL from step 1)* |

4. Click **Save Changes** — Render will redeploy automatically

### Step 3 — Verify it's working

After the redeploy finishes, check the logs. You should see:
```
[DB] Using PostgreSQL (prod)
[DB] PostgreSQL schema ready.
[Benz Bingo] http://0.0.0.0:10000  DB=postgres
```

Then visit:
```
https://benz-bingo-api.onrender.com/health
```
Should return:
```json
{"status": "ok", "db": "postgres"}
```

---

## Option B — Supabase (if you prefer a dashboard UI)

Supabase gives you a visual table editor, which is handy for admin tasks.

### Step 1 — Create a Supabase project

1. Go to **supabase.com** → sign up free
2. Click **New project**
3. Fill in:
   - **Project name**: `benz-bingo`
   - **Database password**: pick a strong password (save it!)
   - **Region**: EU West (closest to Ethiopia)
4. Click **Create new project** — takes ~2 minutes

### Step 2 — Get your connection string

1. In your project → **Settings** (gear icon) → **Database**
2. Scroll to **"Connection string"** → select **URI** tab
3. Copy the string — it looks like:
   ```
   postgresql://postgres:[YOUR-PASSWORD]@db.xxxx.supabase.co:5432/postgres
   ```
4. Replace `[YOUR-PASSWORD]` with the password you set in step 1

### Step 3 — Add to Render

Same as Option A Step 2 — add `DATABASE_URL` to your Render web service environment.

### Supabase bonus — visual admin

Supabase's **Table Editor** lets you browse your users, games, and history visually:
- Go to **Table Editor** in Supabase dashboard
- After your first user registers, you'll see their row in the `users` table
- You can manually edit balances here as an alternative to the bot admin commands

---

## How the Updated app.py Works

The new `app.py` **auto-detects** which database to use:

```
DATABASE_URL not set  →  SQLite (local dev, bingo.db file)
DATABASE_URL = postgresql://...  →  PostgreSQL (production)
```

No code changes needed — just set the environment variable.

### Key technical differences handled automatically

| Feature | SQLite | PostgreSQL |
|---|---|---|
| Placeholders | `?` | `%s` (auto-converted) |
| Auto-increment | `INTEGER PRIMARY KEY AUTOINCREMENT` | `SERIAL PRIMARY KEY` |
| Timestamps | `datetime('now')` | `NOW()` |
| Get inserted ID | `cursor.lastrowid` | `RETURNING id` clause |
| Concurrency | WAL mode | Native MVCC |
| Connection pool | N/A | `ThreadedConnectionPool(1–10)` |

---

## Local Development (still uses SQLite)

When running on your own computer, you don't need PostgreSQL at all:

```bash
# .env file — no DATABASE_URL means SQLite is used automatically
TELEGRAM_BOT_TOKEN=your_token
ADMIN_TG_IDS=your_tg_id
WEBAPP_URL=http://localhost:5000
INTERNAL_SECRET=any-secret
```

```bash
python app.py
# [DB] Using SQLite (local dev)
# [DB] SQLite schema ready.
```

Your data is saved in `bingo.db` in the project folder.

---

## Migrating Existing Data (if needed)

If you already have users in SQLite and want to move them to PostgreSQL:

```bash
# Install pgloader (Linux/Mac)
sudo apt install pgloader   # Ubuntu
brew install pgloader       # Mac

# Run migration
pgloader sqlite:///path/to/bingo.db postgresql://your_connection_string
```

---

## Complete Environment Variables Reference

### Flask web service (`benz-bingo-api`) on Render

| Variable | Required | Value |
|---|---|---|
| `DATABASE_URL` | ✅ Yes | From Render PostgreSQL or Supabase |
| `TELEGRAM_BOT_TOKEN` | ✅ Yes | From @BotFather |
| `ADMIN_TG_IDS` | ✅ Yes | Your Telegram numeric ID |
| `WEBAPP_URL` | ✅ Yes | `https://benz-bingo-api.onrender.com` |
| `INTERNAL_SECRET` | ✅ Yes | Any random 32+ char string |
| `PORT` | Auto | Set automatically by Render |
| `WELCOME_BONUS` | Optional | Default: `100` (ETB for new users) |

### Bot worker (`benz-bingo-bot`) on Render

| Variable | Required | Value |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | ✅ Yes | Same token as Flask |
| `ADMIN_TG_IDS` | ✅ Yes | Same IDs as Flask |
| `WEBAPP_URL` | ✅ Yes | `https://benz-bingo-api.onrender.com` |
| `INTERNAL_SECRET` | ✅ Yes | Same secret as Flask |
| `DATABASE_URL` | ❌ No | Bot doesn't touch DB directly |
| `WEBHOOK_HOST` | ❌ No | Leave blank for polling mode |

---

## Verify Everything Is Connected

After deploying with the database set up, run through this checklist:

```
1. Visit /health → {"status":"ok","db":"postgres"}           ✅
2. Open bot → /start → register → get 100 ETB bonus          ✅
3. Check Render PostgreSQL → Table Editor → see user row      ✅
4. Redeploy Flask service → visit /health → data still there  ✅
5. Admin: /admindeposit <your_id> 500 → balance updates       ✅
```

If step 4 passes, your data is truly persisted. 🎉
