"""
Benz Bingo — Flask Backend  +  Telegram Bot (merged, single process)
======================================================================
Running both Flask and the aiogram bot in one process means you only
need ONE free Render Web Service — no Background Worker required.

How it works:
  • Gunicorn starts with --workers 1  (critical — see Procfile)
  • At module load time, start_bot_thread() spawns a daemon thread
  • That thread owns its own asyncio event loop and runs the bot forever
  • Flask handles HTTP on the main thread pool as normal

Database: auto-detects from DATABASE_URL environment variable.
  • If DATABASE_URL starts with postgres → uses psycopg2 (PostgreSQL)
  • Otherwise                           → uses sqlite3  (local dev)

Environment variables:
  DATABASE_URL          — PostgreSQL connection string (Render sets this automatically)
  TELEGRAM_BOT_TOKEN    — from @BotFather
  ADMIN_TG_IDS          — comma-separated Telegram user IDs
  WELCOME_BONUS         — ETB given to new users (default: 100)
  INTERNAL_SECRET       — shared secret for /internal/notify endpoint
  PORT                  — HTTP port (default: 5000)

Run locally:   python app.py
Run on Render: gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 --timeout 120 app:app
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ workers=1 is REQUIRED
"""

import asyncio
import json
import hashlib
import hmac
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)
from flask import Flask, request, jsonify, g, send_from_directory
from flask_cors import CORS

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('benz_bingo')

# ─── CONFIG ───────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')
CORS(app, resources={r"/api/*": {"origins": "*"}})

BOT_TOKEN        = os.environ.get('TELEGRAM_BOT_TOKEN', os.environ.get('BOT_TOKEN', ''))
ADMIN_TG_IDS_STR = os.environ.get('ADMIN_TG_IDS', '')
ADMIN_TG_IDS     = set(ADMIN_TG_IDS_STR.split(',')) if ADMIN_TG_IDS_STR else set()
ADMIN_TG_IDS_INT = {int(x) for x in ADMIN_TG_IDS if x.strip().isdigit()}
WELCOME_BONUS    = int(os.environ.get('WELCOME_BONUS', 100))
INTERNAL_SECRET  = os.environ.get('INTERNAL_SECRET', 'changeme-please')
HOUSE_CUT        = 0.10
COUNTDOWN_SECS   = 120
CALL_INTERVAL    = 5
BET_AMOUNTS      = [10, 50, 100, 200]
REFERRAL_BONUS   = 50
MIN_DEPOSIT      = 10
MAX_DEPOSIT      = 5_000
MIN_WITHDRAW     = 100
PAYMENT_ACCOUNTS = (
    "🏦 <b>CBE:</b>       <code>1000-123-456-789</code>  (Benz Bingo Ltd)\n"
    "📱 <b>Telebirr:</b>  <code>0911-111-111</code>\n"
    "📱 <b>M-Pesa:</b>    <code>0722-222-222</code>"
)

# Set at bot startup, used in referral links
_bot_username: str = ''

# ══════════════════════════════════════════════════════════════════════════════
#  DATABASE LAYER — auto-switches between SQLite (local) and PostgreSQL (prod)
# ══════════════════════════════════════════════════════════════════════════════
_DATABASE_URL = os.environ.get('DATABASE_URL', '')

# Render gives postgres:// URLs; psycopg2 requires postgresql://
if _DATABASE_URL.startswith('postgres://'):
    _DATABASE_URL = _DATABASE_URL.replace('postgres://', 'postgresql://', 1)

DB_TYPE   = 'postgres' if _DATABASE_URL.startswith('postgresql://') else 'sqlite'
SQLITE_DB = os.environ.get('DB_PATH', 'bingo.db')
PH        = '%s' if DB_TYPE == 'postgres' else '?'   # placeholder character

print(f'[DB] Using {"PostgreSQL" if DB_TYPE == "postgres" else "SQLite"} '
      f'({"prod" if DB_TYPE == "postgres" else "local dev"})')

# ── PostgreSQL connection pool (only used when DB_TYPE == 'postgres') ─────────
if DB_TYPE == 'postgres':
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    _pg_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=10, dsn=_DATABASE_URL
    )
else:
    import sqlite3 as _sqlite3
    _pg_pool = None

# ── Generic connection helpers ─────────────────────────────────────────────────
def _get_conn():
    """Return a raw DB connection. Caller is responsible for returning it."""
    if DB_TYPE == 'postgres':
        return _pg_pool.getconn()
    else:
        conn = _sqlite3.connect(SQLITE_DB, check_same_thread=False)
        conn.row_factory = _sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

def _release_conn(conn):
    if DB_TYPE == 'postgres':
        _pg_pool.putconn(conn)
    else:
        conn.close()

# ── Flask per-request connection (via g) ──────────────────────────────────────
def get_db():
    """Return the per-request DB connection (stored in Flask g)."""
    if '_db' not in g:
        g._db = _get_conn()
        if DB_TYPE == 'postgres':
            g._db.autocommit = False
    return g._db

@app.teardown_appcontext
def close_db(exc):
    conn = g.pop('_db', None)
    if conn is not None:
        if DB_TYPE == 'postgres':
            if exc:
                conn.rollback()
            else:
                conn.commit()
            _pg_pool.putconn(conn)
        else:
            conn.close()

def _cursor(conn):
    """Return a dict-like cursor appropriate for the DB type."""
    if DB_TYPE == 'postgres':
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        conn.row_factory = _sqlite3.Row
        return conn.cursor()

def _row_to_dict(row):
    if row is None:
        return None
    if DB_TYPE == 'postgres':
        return dict(row)
    else:
        return dict(row)

# ── Public query helpers ───────────────────────────────────────────────────────
def query_db(sql, args=(), one=False):
    """Run a SELECT and return list of dicts (or one dict if one=True)."""
    # Replace ? with %s for postgres
    if DB_TYPE == 'postgres':
        sql = sql.replace('?', '%s')
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(sql, args)
    rows = cur.fetchall()
    if DB_TYPE == 'postgres':
        result = [dict(r) for r in rows]
    else:
        result = [dict(r) for r in rows]
    return (_row_to_dict(rows[0]) if rows else None) if one else result

def execute_db(sql, args=()):
    """Run an INSERT/UPDATE/DELETE. Returns lastrowid."""
    if DB_TYPE == 'postgres':
        sql = sql.replace('?', '%s')
        # PostgreSQL RETURNING id for INSERT
        if sql.strip().upper().startswith('INSERT') and 'RETURNING' not in sql.upper():
            sql = sql + ' RETURNING id'
    conn = get_db()
    cur  = _cursor(conn)
    cur.execute(sql, args)
    if DB_TYPE == 'postgres':
        conn.commit()
        if sql.strip().upper().startswith('INSERT'):
            row = cur.fetchone()
            return row['id'] if row else None
        conn.commit()
        return None
    else:
        conn.commit()
        return cur.lastrowid

def execute_many(statements):
    """
    Run multiple SQL statements in one transaction.
    statements: list of (sql, args) tuples.
    Returns last cursor.
    """
    conn = get_db()
    cur  = _cursor(conn)
    last_result = None
    for sql, args in statements:
        if DB_TYPE == 'postgres':
            sql = sql.replace('?', '%s')
        cur.execute(sql, args)
        last_result = cur
    conn.commit()
    return last_result

# ── Standalone connection (for GameEngine thread — no Flask context) ───────────
class StandaloneDB:
    """Context manager for DB operations outside Flask app context."""
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = _get_conn()
        if DB_TYPE == 'postgres':
            self.conn.autocommit = False
        return self

    def __exit__(self, exc_type, *_):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        _release_conn(self.conn)

    def query(self, sql, args=()):
        if DB_TYPE == 'postgres':
            sql = sql.replace('?', '%s')
        cur = _cursor(self.conn)
        cur.execute(sql, args)
        rows = cur.fetchall()
        return [dict(r) for r in rows]

    def one(self, sql, args=()):
        rows = self.query(sql, args)
        return rows[0] if rows else None

    def run(self, sql, args=()):
        if DB_TYPE == 'postgres':
            sql = sql.replace('?', '%s')
            if sql.strip().upper().startswith('INSERT') and 'RETURNING' not in sql.upper():
                sql = sql + ' RETURNING id'
        cur = _cursor(self.conn)
        cur.execute(sql, args)
        if DB_TYPE == 'postgres' and sql.strip().upper().startswith('INSERT'):
            row = cur.fetchone()
            return dict(row)['id'] if row else None
        return None

# ── Schema init ───────────────────────────────────────────────────────────────
def init_db():
    """Create tables if they don't exist. Runs once at startup."""
    if DB_TYPE == 'postgres':
        ddl = """
            CREATE TABLE IF NOT EXISTS users (
                id           SERIAL PRIMARY KEY,
                tg_id        TEXT    UNIQUE NOT NULL,
                username     TEXT    UNIQUE NOT NULL,
                balance      INTEGER NOT NULL DEFAULT 100,
                wins         INTEGER NOT NULL DEFAULT 0,
                games_played INTEGER NOT NULL DEFAULT 0,
                total_won    INTEGER NOT NULL DEFAULT 0,
                total_bet    INTEGER NOT NULL DEFAULT 0,
                is_admin     INTEGER NOT NULL DEFAULT 0,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bingo_games (
                id                   TEXT    PRIMARY KEY,
                bet_amount           INTEGER NOT NULL,
                status               TEXT    NOT NULL DEFAULT 'waiting',
                called_numbers       TEXT    NOT NULL DEFAULT '[]',
                number_pool          TEXT    NOT NULL DEFAULT '[]',
                countdown_started_at DOUBLE PRECISION,
                started_at           DOUBLE PRECISION,
                finished_at          DOUBLE PRECISION,
                winner_id            INTEGER REFERENCES users(id),
                created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bingo_players (
                id            SERIAL PRIMARY KEY,
                game_id       TEXT    NOT NULL REFERENCES bingo_games(id),
                user_id       INTEGER NOT NULL REFERENCES users(id),
                picked_number INTEGER,
                card          TEXT,
                marked_cells  TEXT    NOT NULL DEFAULT '[[2,2]]',
                has_bingo     INTEGER NOT NULL DEFAULT 0,
                joined_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(game_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS game_history (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL REFERENCES users(id),
                game_id     TEXT    NOT NULL,
                bet_amount  INTEGER,
                prize_won   INTEGER NOT NULL DEFAULT 0,
                won         INTEGER NOT NULL DEFAULT 0,
                played_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_users_tg_id    ON users(tg_id);
            CREATE INDEX IF NOT EXISTS idx_games_status   ON bingo_games(status);
            CREATE INDEX IF NOT EXISTS idx_players_game   ON bingo_players(game_id);
            CREATE INDEX IF NOT EXISTS idx_history_user   ON game_history(user_id);
        """
        conn = _get_conn()
        try:
            cur = conn.cursor()
            cur.execute(ddl)
            conn.commit()
            print('[DB] PostgreSQL schema ready.')
        finally:
            _release_conn(conn)
    else:
        import sqlite3
        with sqlite3.connect(SQLITE_DB) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_id        TEXT    UNIQUE NOT NULL,
                    username     TEXT    UNIQUE NOT NULL,
                    balance      INTEGER DEFAULT 100,
                    wins         INTEGER DEFAULT 0,
                    games_played INTEGER DEFAULT 0,
                    total_won    INTEGER DEFAULT 0,
                    total_bet    INTEGER DEFAULT 0,
                    is_admin     INTEGER DEFAULT 0,
                    created_at   TEXT    DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS bingo_games (
                    id                   TEXT PRIMARY KEY,
                    bet_amount           INTEGER NOT NULL,
                    status               TEXT    DEFAULT 'waiting',
                    called_numbers       TEXT    DEFAULT '[]',
                    number_pool          TEXT    DEFAULT '[]',
                    countdown_started_at REAL,
                    started_at           REAL,
                    finished_at          REAL,
                    winner_id            INTEGER,
                    created_at           TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (winner_id) REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS bingo_players (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    game_id       TEXT    NOT NULL,
                    user_id       INTEGER NOT NULL,
                    picked_number INTEGER,
                    card          TEXT,
                    marked_cells  TEXT    DEFAULT '[[2,2]]',
                    has_bingo     INTEGER DEFAULT 0,
                    joined_at     TEXT    DEFAULT (datetime('now')),
                    UNIQUE(game_id, user_id),
                    FOREIGN KEY (game_id) REFERENCES bingo_games(id),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
                CREATE TABLE IF NOT EXISTS game_history (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id    INTEGER NOT NULL,
                    game_id    TEXT    NOT NULL,
                    bet_amount INTEGER,
                    prize_won  INTEGER DEFAULT 0,
                    won        INTEGER DEFAULT 0,
                    played_at  TEXT    DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                );
                CREATE INDEX IF NOT EXISTS idx_users_tg_id  ON users(tg_id);
                CREATE INDEX IF NOT EXISTS idx_games_status ON bingo_games(status);
                CREATE INDEX IF NOT EXISTS idx_players_game ON bingo_players(game_id);
                CREATE INDEX IF NOT EXISTS idx_history_user ON game_history(user_id);
            """)
            conn.commit()
            print('[DB] SQLite schema ready.')

# ══════════════════════════════════════════════════════════════════════════════
#  AUTH HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def verify_telegram_init_data(init_data: str):
    try:
        parts = {}
        for seg in init_data.split('&'):
            if '=' in seg:
                k, v = seg.split('=', 1)
                parts[k] = v
        recv_hash = parts.pop('hash', '')
        data_str  = '\n'.join(f'{k}={v}' for k, v in sorted(parts.items()))
        secret    = hmac.new(b'WebAppData', BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc_hash = hmac.new(secret, data_str.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc_hash, recv_hash):
            return json.loads(parts.get('user', '{}'))
    except Exception:
        pass
    return None

def get_tg_user_from_request():
    init_data = request.headers.get('X-Telegram-Init-Data', '')
    if init_data and BOT_TOKEN:
        user = verify_telegram_init_data(init_data)
        if user:
            return user
    dev_hdr = request.headers.get('X-Tg-User', '')
    if dev_hdr:
        try:
            return json.loads(dev_hdr)
        except Exception:
            pass
    return None

def get_current_user():
    tg = get_tg_user_from_request()
    if not tg:
        return None, None
    tg_id = str(tg.get('id', ''))
    if not tg_id:
        return None, None
    row = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    return row, tg

def require_auth():
    user, tg = get_current_user()
    if not user:
        return None, None, (jsonify({'error': 'Unauthorized'}), 401)
    return user, tg, None

def require_admin():
    user, tg, err = require_auth()
    if err:
        return None, err
    if not user.get('is_admin'):
        return None, (jsonify({'error': 'Admin access required'}), 403)
    return user, None

# ══════════════════════════════════════════════════════════════════════════════
#  BINGO LOGIC
# ══════════════════════════════════════════════════════════════════════════════
def generate_bingo_card(seed: int) -> list:
    rng    = random.Random(seed ^ int(time.time() * 10) % 0xFFFF)
    ranges = [(1,20),(21,40),(41,60),(61,80),(81,100)]
    cols   = []
    for lo, hi in ranges:
        pool = list(range(lo, hi + 1))
        rng.shuffle(pool)
        cols.append(pool[:5])
    card = [[cols[c][r] for c in range(5)] for r in range(5)]
    card[2][2] = 'FREE'
    return card

def check_bingo(card: list, marked: list) -> bool:
    s = {(r, c) for r, c in marked}
    for r in range(5):
        if all((r, c) in s for c in range(5)): return True
    for c in range(5):
        if all((r, c) in s for r in range(5)): return True
    if all((i, i) in s for i in range(5)):     return True
    if all((i, 4-i) in s for i in range(5)):   return True
    return False

# ══════════════════════════════════════════════════════════════════════════════
#  GAME ENGINE  (background thread — no Flask context)
# ══════════════════════════════════════════════════════════════════════════════
_db_lock = threading.Lock()

class GameEngine:
    def __init__(self):
        t = threading.Thread(target=self._loop, daemon=True, name='GameEngine')
        t.start()

    def _loop(self):
        while True:
            try:
                self._tick()
            except Exception as exc:
                print(f'[Engine] ERROR: {exc}')
            time.sleep(1)

    def _tick(self):
        now = time.time()
        with _db_lock, StandaloneDB() as db:
            games = db.query(
                "SELECT * FROM bingo_games WHERE status IN ('waiting','countdown','active')"
            )
            for g in games:
                gid    = g['id']
                status = g['status']
                pc_row = db.one(
                    'SELECT COUNT(*) AS c FROM bingo_players WHERE game_id=?', (gid,)
                )
                pcount = pc_row['c'] if pc_row else 0

                # waiting → countdown
                if status == 'waiting' and pcount >= 2:
                    db.run(
                        "UPDATE bingo_games SET status='countdown', countdown_started_at=? WHERE id=?",
                        (now, gid)
                    )
                    print(f'[Engine] {gid}: COUNTDOWN ({pcount} players)')

                # countdown
                elif status == 'countdown':
                    if pcount < 2:
                        db.run(
                            "UPDATE bingo_games SET status='waiting', countdown_started_at=NULL WHERE id=?",
                            (gid,)
                        )
                    elif g['countdown_started_at'] and (now - g['countdown_started_at']) >= COUNTDOWN_SECS:
                        pool = list(range(1, 101))
                        random.shuffle(pool)
                        db.run(
                            "UPDATE bingo_games SET status='active', started_at=?, number_pool=? WHERE id=?",
                            (now, json.dumps(pool), gid)
                        )
                        print(f'[Engine] {gid}: ACTIVE')

                # active — call next number(s)
                elif status == 'active':
                    called = json.loads(g['called_numbers'] or '[]')
                    pool   = json.loads(g['number_pool']   or '[]')
                    if not pool:
                        # Exhausted — refund all
                        players = db.query(
                            'SELECT * FROM bingo_players WHERE game_id=?', (gid,)
                        )
                        for p in players:
                            db.run(
                                'UPDATE users SET balance=balance+? WHERE id=?',
                                (g['bet_amount'], p['user_id'])
                            )
                        db.run(
                            "UPDATE bingo_games SET status='finished', finished_at=? WHERE id=?",
                            (now, gid)
                        )
                        print(f'[Engine] {gid}: FINISHED (exhausted, refunded)')
                        continue

                    started  = g['started_at'] or now
                    expected = int((now - started) // CALL_INTERVAL) + 1
                    while len(called) < expected and pool:
                        nxt = pool.pop(0)
                        called.append(nxt)
                        print(f'[Engine] {gid}: called {nxt} ({len(called)}/100)')

                    db.run(
                        'UPDATE bingo_games SET called_numbers=?, number_pool=? WHERE id=?',
                        (json.dumps(called), json.dumps(pool), gid)
                    )

# ══════════════════════════════════════════════════════════════════════════════
#  API ROUTES  (unchanged logic — just using new query helpers)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def serve_index():
    return send_from_directory('static', 'index.html')

@app.route('/api/auth', methods=['POST'])
def auth():
    body    = request.json or {}
    tg_user = body.get('tg_user') or get_tg_user_from_request()
    if not tg_user:
        return jsonify({'error': 'No Telegram user data'}), 400
    tg_id = str(tg_user.get('id', ''))
    if not tg_id:
        return jsonify({'error': 'Missing user id'}), 400
    row = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    if row:
        return jsonify({'status': 'existing', 'user': row})
    return jsonify({
        'status': 'new',
        'suggested_username': tg_user.get('username') or tg_user.get('first_name', 'Player'),
        'tg_id': tg_id
    })

@app.route('/api/register', methods=['POST'])
def register():
    body     = request.json or {}
    tg_id    = str(body.get('tg_id', '')).strip()
    username = body.get('username', '').strip()
    if not tg_id or not username:
        return jsonify({'error': 'tg_id and username are required'}), 400
    if len(username) < 2 or len(username) > 20:
        return jsonify({'error': 'Username must be 2-20 characters'}), 400
    if query_db('SELECT id FROM users WHERE username=?', (username,), one=True):
        return jsonify({'error': 'Username already taken'}), 409
    if query_db('SELECT id FROM users WHERE tg_id=?', (tg_id,), one=True):
        return jsonify({'error': 'Telegram account already registered'}), 409
    is_admin = 1 if tg_id in ADMIN_TG_IDS else 0
    execute_db(
        'INSERT INTO users (tg_id, username, balance, is_admin) VALUES (?,?,?,?)',
        (tg_id, username, WELCOME_BONUS, is_admin)
    )
    user = query_db('SELECT * FROM users WHERE tg_id=?', (tg_id,), one=True)
    return jsonify({'status': 'registered', 'user': user}), 201

@app.route('/api/user', methods=['GET'])
def get_user():
    user, _, err = require_auth()
    if err: return err
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    return jsonify(fresh)

@app.route('/api/games', methods=['GET'])
def list_games():
    bet    = request.args.get('bet', type=int)
    sql    = """
        SELECT g.*, COUNT(p.id) as player_count
        FROM bingo_games g
        LEFT JOIN bingo_players p ON g.id=p.game_id
        WHERE g.status IN ('waiting','countdown','active')
    """
    params = []
    if bet:
        sql += ' AND g.bet_amount=?'
        params.append(bet)
    sql += ' GROUP BY g.id ORDER BY g.created_at DESC'
    games = query_db(sql, params)
    now   = time.time()
    for gm in games:
        gm['pot']   = gm['player_count'] * gm['bet_amount']
        gm['prize'] = int(gm['pot'] * (1 - HOUSE_CUT))
        csat = gm.get('countdown_started_at')
        gm['countdown_remaining'] = (
            max(0, COUNTDOWN_SECS - int(now - csat))
            if gm['status'] == 'countdown' and csat else COUNTDOWN_SECS
        )
        players = query_db(
            'SELECT u.username, bp.picked_number FROM bingo_players bp '
            'JOIN users u ON bp.user_id=u.id WHERE bp.game_id=?', (gm['id'],)
        )
        gm['players'] = players
    return jsonify(games)

@app.route('/api/games', methods=['POST'])
def create_game():
    user, _, err = require_auth()
    if err: return err
    body = request.json or {}
    bet  = body.get('bet_amount')
    if bet not in BET_AMOUNTS:
        return jsonify({'error': f'bet_amount must be one of {BET_AMOUNTS}'}), 400
    if user['balance'] < bet:
        return jsonify({'error': 'Insufficient balance'}), 400
    gid = 'G' + uuid.uuid4().hex[:8].upper()
    execute_db('INSERT INTO bingo_games (id, bet_amount) VALUES (?,?)', (gid, bet))
    game = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    return jsonify(game), 201

@app.route('/api/games/<gid>', methods=['GET'])
def get_game(gid):
    user, _, err = require_auth()
    if err: return err
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    gm['called_numbers'] = json.loads(gm['called_numbers'] or '[]')
    now  = time.time()
    csat = gm.get('countdown_started_at')
    gm['countdown_remaining'] = (
        max(0, COUNTDOWN_SECS - int(now - csat))
        if gm['status'] == 'countdown' and csat else COUNTDOWN_SECS
    )
    players = query_db(
        'SELECT u.username, bp.picked_number, bp.has_bingo '
        'FROM bingo_players bp JOIN users u ON bp.user_id=u.id WHERE bp.game_id=?', (gid,)
    )
    gm['players']      = players
    gm['player_count'] = len(players)
    gm['pot']          = gm['player_count'] * gm['bet_amount']
    gm['prize']        = int(gm['pot'] * (1 - HOUSE_CUT))
    prow = query_db(
        'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
    )
    if prow:
        gm['my_player'] = {
            'card':         json.loads(prow['card'] or '[]'),
            'marked_cells': json.loads(prow['marked_cells'] or '[[2,2]]'),
            'has_bingo':    prow['has_bingo'],
        }
    return jsonify(gm)

@app.route('/api/games/<gid>/join', methods=['POST'])
def join_game(gid):
    user, _, err = require_auth()
    if err: return err
    body   = request.json or {}
    picked = body.get('picked_number')
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    if gm['status'] not in ('waiting', 'countdown'):
        return jsonify({'error': 'Game already started or finished'}), 400
    existing = query_db(
        'SELECT id FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
    )
    if existing:
        prow = query_db(
            'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
        )
        return jsonify({'status': 'already_joined', 'card': json.loads(prow['card'])}), 200
    if picked:
        taken = query_db(
            'SELECT id FROM bingo_players WHERE game_id=? AND picked_number=?', (gid, picked), one=True
        )
        if taken:
            return jsonify({'error': 'That number is already taken'}), 409
    fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    if fresh['balance'] < gm['bet_amount']:
        return jsonify({'error': 'Insufficient balance'}), 400
    card = generate_bingo_card(picked or random.randint(1, 100))
    with _db_lock:
        execute_db(
            'UPDATE users SET balance=balance-?, total_bet=total_bet+? WHERE id=?',
            (gm['bet_amount'], gm['bet_amount'], user['id'])
        )
        execute_db(
            'INSERT INTO bingo_players (game_id, user_id, picked_number, card) VALUES (?,?,?,?)',
            (gid, user['id'], picked, json.dumps(card))
        )
    return jsonify({'status': 'joined', 'card': card, 'marked_cells': [[2, 2]]})

@app.route('/api/games/<gid>/mark', methods=['POST'])
def mark_cell(gid):
    user, _, err = require_auth()
    if err: return err
    body = request.json or {}
    r, c = body.get('row'), body.get('col')
    if r is None or c is None:
        return jsonify({'error': 'row and col required'}), 400
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm or gm['status'] != 'active':
        return jsonify({'error': 'Game not active'}), 400
    prow = query_db(
        'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
    )
    if not prow:
        return jsonify({'error': 'You are not in this game'}), 403
    card   = json.loads(prow['card'])
    called = json.loads(gm['called_numbers'] or '[]')
    val    = card[r][c]
    if val != 'FREE' and val not in called:
        return jsonify({'error': f'Number {val} has not been called yet'}), 400
    marked = json.loads(prow['marked_cells'])
    if [r, c] not in marked:
        marked.append([r, c])
        with _db_lock:
            execute_db(
                'UPDATE bingo_players SET marked_cells=? WHERE id=?',
                (json.dumps(marked), prow['id'])
            )
    return jsonify({'marked_cells': marked})

@app.route('/api/games/<gid>/claim', methods=['POST'])
def claim_bingo(gid):
    user, _, err = require_auth()
    if err: return err
    with _db_lock:
        gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
        if not gm:
            return jsonify({'error': 'Game not found'}), 404
        if gm['status'] != 'active':
            return jsonify({'error': 'Game not active'}), 400
        if gm['winner_id']:
            return jsonify({'error': 'A winner has already been declared'}), 400
        prow = query_db(
            'SELECT * FROM bingo_players WHERE game_id=? AND user_id=?', (gid, user['id']), one=True
        )
        if not prow:
            return jsonify({'error': 'You are not in this game'}), 403
        card   = json.loads(prow['card'])
        marked = json.loads(prow['marked_cells'])
        if not check_bingo(card, marked):
            return jsonify({'error': 'No valid BINGO on your card!'}), 400
        pcount = query_db(
            'SELECT COUNT(*) AS c FROM bingo_players WHERE game_id=?', (gid,), one=True
        )['c']
        pot   = pcount * gm['bet_amount']
        prize = int(pot * (1 - HOUSE_CUT))
        now   = time.time()
        execute_many([
            ("UPDATE bingo_games SET status='finished', winner_id=?, finished_at=? WHERE id=?",
             (user['id'], now, gid)),
            ('UPDATE bingo_players SET has_bingo=1 WHERE id=?', (prow['id'],)),
            ('UPDATE users SET balance=balance+?, wins=wins+1, games_played=games_played+1, '
             'total_won=total_won+? WHERE id=?', (prize, prize, user['id'])),
            ('INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won) VALUES (?,?,?,?,1)',
             (user['id'], gid, gm['bet_amount'], prize)),
        ])
        losers = query_db(
            'SELECT user_id FROM bingo_players WHERE game_id=? AND user_id!=?',
            (gid, user['id'])
        )
        for lo in losers:
            execute_many([
                ('UPDATE users SET games_played=games_played+1 WHERE id=?', (lo['user_id'],)),
                ('INSERT INTO game_history (user_id,game_id,bet_amount,prize_won,won) VALUES (?,?,?,0,0)',
                 (lo['user_id'], gid, gm['bet_amount'])),
            ])
        fresh = query_db('SELECT * FROM users WHERE id=?', (user['id'],), one=True)
    return jsonify({'winner': True, 'prize': prize, 'user': fresh})

@app.route('/api/history', methods=['GET'])
def get_history():
    user, _, err = require_auth()
    if err: return err
    rows = query_db(
        'SELECT * FROM game_history WHERE user_id=? ORDER BY played_at DESC LIMIT 50',
        (user['id'],)
    )
    return jsonify(rows)

@app.route('/api/leaderboard', methods=['GET'])
def leaderboard():
    rows = query_db(
        'SELECT username,wins,games_played,total_won,balance FROM users '
        'ORDER BY wins DESC, total_won DESC LIMIT 20'
    )
    return jsonify(rows)

# ── Admin routes ──────────────────────────────────────────────────────────────
@app.route('/api/admin/stats', methods=['GET'])
def admin_stats():
    user, err = require_admin()
    if err: return err
    stats = {
        'total_users':    query_db('SELECT COUNT(*) AS c FROM users', one=True)['c'],
        'total_games':    query_db('SELECT COUNT(*) AS c FROM bingo_games', one=True)['c'],
        'active_games':   query_db(
            "SELECT COUNT(*) AS c FROM bingo_games WHERE status IN ('waiting','countdown','active')",
            one=True)['c'],
        'finished_games': query_db(
            "SELECT COUNT(*) AS c FROM bingo_games WHERE status='finished'", one=True)['c'],
        'total_prizes':   query_db(
            'SELECT COALESCE(SUM(prize_won),0) AS s FROM game_history', one=True)['s'],
    }
    return jsonify(stats)

@app.route('/api/admin/users', methods=['GET'])
def admin_users():
    user, err = require_admin()
    if err: return err
    return jsonify(query_db('SELECT * FROM users ORDER BY created_at DESC'))

@app.route('/api/admin/games', methods=['GET'])
def admin_games():
    user, err = require_admin()
    if err: return err
    rows = query_db("""
        SELECT g.*, COUNT(p.id) as player_count
        FROM bingo_games g LEFT JOIN bingo_players p ON g.id=p.game_id
        GROUP BY g.id ORDER BY g.created_at DESC LIMIT 100
    """)
    return jsonify(rows)

@app.route('/api/admin/funds', methods=['POST'])
def admin_funds():
    admin, err = require_admin()
    if err: return err
    body     = request.json or {}
    username = body.get('username', '').strip()
    amount   = body.get('amount', 0)
    target   = query_db('SELECT * FROM users WHERE username=?', (username,), one=True)
    if not target:
        return jsonify({'error': 'User not found'}), 404
    new_bal = max(0, target['balance'] + int(amount))
    execute_db('UPDATE users SET balance=? WHERE username=?', (new_bal, username))
    return jsonify({'success': True, 'username': username, 'new_balance': new_bal})

@app.route('/api/admin/games/<gid>/finish', methods=['POST'])
def admin_force_finish(gid):
    admin, err = require_admin()
    if err: return err
    gm = query_db('SELECT * FROM bingo_games WHERE id=?', (gid,), one=True)
    if not gm:
        return jsonify({'error': 'Game not found'}), 404
    if gm['status'] == 'finished':
        return jsonify({'error': 'Already finished'}), 400
    with _db_lock:
        players = query_db('SELECT * FROM bingo_players WHERE game_id=?', (gid,))
        for p in players:
            execute_db('UPDATE users SET balance=balance+? WHERE id=?', (gm['bet_amount'], p['user_id']))
        execute_db(
            "UPDATE bingo_games SET status='finished', finished_at=? WHERE id=?",
            (time.time(), gid)
        )
    return jsonify({'success': True, 'refunded': len(players)})

@app.route('/api/admin/users/<int:uid>/admin', methods=['POST'])
def toggle_admin(uid):
    admin, err = require_admin()
    if err: return err
    is_adm = 1 if (request.json or {}).get('is_admin') else 0
    execute_db('UPDATE users SET is_admin=? WHERE id=?', (is_adm, uid))
    return jsonify({'success': True})

# ── Health check (for UptimeRobot / Render) ───────────────────────────────────
@app.route('/health')
def health():
    return jsonify({'status': 'ok', 'db': DB_TYPE})

# ── Internal notify endpoint (bot calls this to push Telegram messages) ────────
@app.route('/internal/notify', methods=['POST'])
def internal_notify():
    if request.headers.get('X-Internal-Secret') != INTERNAL_SECRET:
        return jsonify({'error': 'Unauthorized'}), 401
    # Notification is handled by the bot thread; we just queue it
    data   = request.json or {}
    tg_id  = data.get('tg_id')
    event  = data.get('event', '')
    if not tg_id:
        return jsonify({'error': 'tg_id required'}), 400
    _notify_queue.put({'tg_id': int(tg_id), 'event': event, 'data': data})
    return jsonify({'ok': True})

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM BOT — runs in a background daemon thread inside this same process
#
#  IMPORTANT: Gunicorn must use --workers 1
#             Multiple workers = multiple bot polling instances = Telegram error
# ══════════════════════════════════════════════════════════════════════════════

import queue as _queue_module
_notify_queue: _queue_module.Queue = _queue_module.Queue()

# ── FSM States ────────────────────────────────────────────────────────────────
class Registration(StatesGroup):
    waiting_for_username = State()
    waiting_for_phone    = State()

class Deposit(StatesGroup):
    waiting_for_amount  = State()
    waiting_for_sms_ref = State()

class Withdrawal(StatesGroup):
    waiting_for_amount = State()
    waiting_for_phone  = State()

# ── Bot API helper (calls Flask REST API — same host, loopback) ───────────────
async def bot_api(method: str, path: str, tg_id: int | None = None,
                  body: dict | None = None,
                  session: aiohttp.ClientSession | None = None) -> dict:
    """
    Call the local Flask API from inside the bot.
    Uses localhost so traffic never leaves the machine — zero latency.
    """
    port    = int(os.environ.get('PORT', 5000))
    base    = f'http://127.0.0.1:{port}'
    headers = {'Content-Type': 'application/json'}
    if tg_id:
        headers['X-Tg-User'] = json.dumps({'id': tg_id})

    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()
    try:
        async with session.request(method, base + path, json=body, headers=headers) as resp:
            data = await resp.json(content_type=None)
            if not resp.ok:
                raise ValueError(data.get('error', f'API {resp.status}'))
            return data
    finally:
        if own_session:
            await session.close()

# ── Notification helper ───────────────────────────────────────────────────────
async def bot_notify(bot: Bot, tg_id: int, text: str) -> bool:
    try:
        await bot.send_message(chat_id=tg_id, text=text, parse_mode=ParseMode.HTML)
        return True
    except Exception as exc:
        logger.warning(f'[notify] failed for {tg_id}: {exc}')
        return False

# ── Keyboards ─────────────────────────────────────────────────────────────────
def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='🎮 Play Bingo')],
            [KeyboardButton(text='💰 Deposit'),  KeyboardButton(text='💳 Withdraw')],
            [KeyboardButton(text='📊 My Stats'), KeyboardButton(text='🏆 Leaderboard')],
        ],
        resize_keyboard=True,
        input_field_placeholder='Choose an option…',
    )

def kb_cancel() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text='❌ Cancel')]],
        resize_keyboard=True, one_time_keyboard=True,
    )

def kb_phone() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text='📱 Share Phone Number', request_contact=True)],
            [KeyboardButton(text='⏭ Skip')],
        ],
        resize_keyboard=True, one_time_keyboard=True,
    )

def kb_webapp(label: str = '🎱 Open Benz Bingo', bet: int | None = None) -> InlineKeyboardMarkup:
    port = int(os.environ.get('PORT', 5000))
    base = os.environ.get('WEBAPP_URL', f'http://localhost:{port}')
    url  = f'{base}?bet={bet}' if bet else base
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=label, web_app=WebAppInfo(url=url))
    ]])

def kb_bets(balance: int) -> InlineKeyboardMarkup:
    rows = []
    pair = []
    for a in BET_AMOUNTS:
        label = f'🪙 {a} ETB' + (' ✓' if balance >= a else ' 🔒')
        pair.append(InlineKeyboardButton(text=label, callback_data=f'bet:{a}'))
        if len(pair) == 2:
            rows.append(pair); pair = []
    if pair:
        rows.append(pair)
    rows.append([InlineKeyboardButton(text='❌ Cancel', callback_data='bet:cancel')])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ── Router ────────────────────────────────────────────────────────────────────
bot_router = Router(name='benz_bingo')

def admin_guard(handler):
    async def _w(message: Message, *a, **kw):
        if message.from_user.id not in ADMIN_TG_IDS_INT:
            await message.answer('⛔ Admin access required.')
            return
        return await handler(message, *a, **kw)
    _w.__name__ = handler.__name__
    return _w

# /start ──────────────────────────────────────────────────────────────────────
@bot_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    await state.clear()
    tg   = message.from_user
    args = message.text.split()[1:]
    ref  = int(args[0]) if args and args[0].isdigit() and int(args[0]) != tg.id else None
    if ref:
        await state.update_data(referrer_id=ref)
    try:
        res = await bot_api('POST', '/api/auth', tg_id=tg.id,
                            body={'tg_user': {'id': tg.id, 'username': tg.username,
                                              'first_name': tg.first_name}},
                            session=session)
    except Exception as exc:
        await message.answer(f'⚠️ Server error: {exc}'); return

    if res.get('status') == 'existing':
        u = res['user']
        await message.answer(
            f"👋 Welcome back, <b>{u['username']}</b>!\n\n"
            f"💰 Balance: <b>{u['balance']:,} ETB</b>\n"
            f"🏆 Wins: {u['wins']}  ·  🎮 Games: {u['games_played']}",
            reply_markup=kb_main(),
        )
    else:
        suggested = res.get('suggested_username') or tg.first_name or 'Player'
        await state.set_state(Registration.waiting_for_username)
        await state.update_data(tg_id=str(tg.id))
        await message.answer(
            '🎱 <b>Welcome to Benz Bingo!</b>\n\n'
            'Pick numbers · Mark your card · Shout <b>BINGO!</b>\n'
            'Win up to <b>90% of the pot</b> every round 💰\n\n'
            f'Choose a username (2–20 chars, no spaces):\n'
            f'<i>Suggestion:</i> <code>{suggested}</code>',
            reply_markup=kb_cancel(),
        )

# Registration FSM ────────────────────────────────────────────────────────────
@bot_router.message(Registration.waiting_for_username, F.text)
async def reg_username(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Registration cancelled.', reply_markup=ReplyKeyboardRemove()); return
    u = message.text.strip()
    if len(u) < 2 or len(u) > 20 or ' ' in u:
        await message.answer('⚠️ 2–20 chars, no spaces. Try again:'); return
    data = await state.get_data()
    try:
        res  = await bot_api('POST', '/api/register', tg_id=int(data['tg_id']),
                             body={'tg_id': data['tg_id'], 'username': u}, session=session)
    except ValueError as exc:
        await message.answer(f'⚠️ {exc}\nTry a different username:'); return
    user = res['user']

    # Referral bonus
    ref = data.get('referrer_id')
    if ref:
        try:
            ref_users = await bot_api('GET', '/api/admin/users', tg_id=int(data['tg_id']),
                                     session=session)
            ref_user  = next((x for x in ref_users if x['tg_id'] == str(ref)), None)
            if ref_user:
                await bot_api('POST', '/api/admin/funds', tg_id=ref,
                              body={'username': ref_user['username'], 'amount': REFERRAL_BONUS},
                              session=session)
        except Exception:
            pass

    await state.set_state(Registration.waiting_for_phone)
    await message.answer(
        f"✅ <b>Account created!</b>\n\n"
        f"Username: <b>{user['username']}</b>\n"
        f"Welcome bonus: <b>100 ETB</b> 🎁\n\n"
        "📱 Share your phone to enable deposits/withdrawals.\n"
        "(Tap <b>⏭ Skip</b> to do this later.)",
        reply_markup=kb_phone(),
    )

@bot_router.message(Registration.waiting_for_phone, F.contact)
async def reg_phone(message: Message, state: FSMContext):
    if message.contact.user_id != message.from_user.id:
        await message.answer('⚠️ Share <b>your own</b> contact.'); return
    await state.clear()
    link = f'https://t.me/{_bot_username}?start={message.from_user.id}'
    await message.answer(
        f'✅ <b>Phone verified!</b>\n\n'
        f'🔗 Referral link:\n<code>{link}</code>\n\n'
        f'Earn <b>{REFERRAL_BONUS} ETB</b> per friend who joins! 🎉',
        reply_markup=kb_main(),
    )

@bot_router.message(Registration.waiting_for_phone, F.text.in_({'⏭ Skip', '❌ Cancel'}))
async def reg_skip(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("👍 You can add your phone later.\nLet's play!", reply_markup=kb_main())

# /cancel  ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('cancel'))
@bot_router.message(F.text == '❌ Cancel')
async def cmd_cancel(message: Message, state: FSMContext):
    cur = await state.get_state()
    await state.clear()
    txt = '✅ Cancelled.' if cur else 'Nothing to cancel.'
    await message.answer(txt, reply_markup=kb_main())

# /help ───────────────────────────────────────────────────────────────────────
@bot_router.message(Command('help'))
async def cmd_help(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        '🎱 <b>Benz Bingo — How to Play</b>\n\n'
        '1. Tap 🎮 Play Bingo → choose bet (10/50/100/200 ETB)\n'
        '2. Pick a lucky number 1–100\n'
        '3. Get your unique 5×5 BINGO card\n'
        '4. 2+ players join → 2-min countdown → GO!\n'
        '5. Numbers called every 5 seconds\n'
        '6. Tap called numbers to mark them\n'
        '7. Complete row / column / diagonal → 🎉 BINGO!\n'
        '8. Winner takes <b>90% of the pot</b>\n\n'
        '<b>Commands:</b> /start /balance /stats /history /referral /cancel /help',
    )

# /balance ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('balance'))
async def cmd_balance(message: Message, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await message.answer(
        f"💰 <b>Wallet — {u['username']}</b>\n\n"
        f"Balance:   <b>{u['balance']:,} ETB</b>\n"
        f"Total won: {u['total_won']:,} ETB\n"
        f"Total bet: {u['total_bet']:,} ETB",
    )

# 📊 My Stats ─────────────────────────────────────────────────────────────────
@bot_router.message(Command('stats'))
@bot_router.message(F.text == '📊 My Stats')
async def cmd_stats(message: Message, session: aiohttp.ClientSession):
    try:
        u   = await bot_api('GET', '/api/user',    tg_id=message.from_user.id, session=session)
        his = await bot_api('GET', '/api/history', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    gp  = u['games_played'] or 1
    wr  = round(u['wins'] / gp * 100, 1)
    rec = ''.join(
        f"{'✅' if h['won'] else '❌'} {'+' if h['won'] else '−'}"
        f"{h['prize_won'] if h['won'] else h['bet_amount']:,} ETB · {str(h['played_at'])[:10]}\n"
        for h in his[:5]
    ) or '<i>No games yet</i>'
    await message.answer(
        f"📊 <b>Stats — {u['username']}</b>\n\n"
        f"💰 Balance:      <b>{u['balance']:,} ETB</b>\n"
        f"🎮 Games played: {u['games_played']}\n"
        f"🏆 Wins:         {u['wins']}\n"
        f"📈 Win rate:     {wr}%\n"
        f"💵 Total won:    {u['total_won']:,} ETB\n\n"
        f"<b>Last 5 games:</b>\n{rec}",
    )

# /history ────────────────────────────────────────────────────────────────────
@bot_router.message(Command('history'))
async def cmd_history(message: Message, session: aiohttp.ClientSession):
    try:
        his = await bot_api('GET', '/api/history', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    if not his:
        await message.answer('📜 No games yet — tap 🎮 Play Bingo to start!'); return
    lines = [
        f"{'🏆' if h['won'] else '💸'} <b>{'+' if h['won'] else '−'}"
        f"{h['prize_won'] if h['won'] else h['bet_amount']:,} ETB</b>"
        f" · {h['bet_amount']} bet · {str(h['played_at'])[:10]}"
        for h in his[:20]
    ]
    await message.answer('📜 <b>Game History (last 20)</b>\n\n' + '\n'.join(lines))

# /referral ───────────────────────────────────────────────────────────────────
@bot_router.message(Command('referral'))
async def cmd_referral(message: Message):
    link = f'https://t.me/{_bot_username}?start={message.from_user.id}'
    await message.answer(
        f'🔗 <b>Your Referral Link</b>\n\n<code>{link}</code>\n\n'
        f'Earn <b>{REFERRAL_BONUS} ETB</b> for every friend who joins and plays! 🎁',
    )

# 🏆 Leaderboard ──────────────────────────────────────────────────────────────
@bot_router.message(F.text == '🏆 Leaderboard')
async def cmd_leaderboard(message: Message, session: aiohttp.ClientSession):
    try:
        rows = await bot_api('GET', '/api/leaderboard', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    medals = ['🥇', '🥈', '🥉']
    lines  = [
        f"{medals[i] if i < 3 else f'{i+1}.'} <b>{u['username']}</b>"
        f" — {u['wins']}W · {u['total_won']:,} ETB"
        for i, u in enumerate(rows[:10])
    ]
    await message.answer('🏆 <b>Top 10 Players</b>\n\n' + '\n'.join(lines) if lines else 'No players yet.')

# 🎮 Play Bingo ───────────────────────────────────────────────────────────────
@bot_router.message(F.text == '🎮 Play Bingo')
async def cmd_play(message: Message, session: aiohttp.ClientSession):
    try:
        u     = await bot_api('GET', '/api/user',  tg_id=message.from_user.id, session=session)
        games = await bot_api('GET', '/api/games', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    counts: dict[int, int] = {b: 0 for b in BET_AMOUNTS}
    for gm in games:
        b = gm.get('bet_amount')
        if b in counts and gm.get('status') in ('waiting', 'countdown'):
            counts[b] += gm.get('player_count', 0)
    lobby = '\n'.join(
        f"  {'🟢' if counts[b] else '⚪'} <b>{b} ETB</b>  ({counts[b]} waiting)"
        for b in BET_AMOUNTS
    )
    await message.answer(
        f"🎯 <b>Choose Your Bet</b>\n\n"
        f"💰 Balance: <b>{u['balance']:,} ETB</b>\n\n"
        f"<b>Active lobbies:</b>\n{lobby}\n\n"
        f"<i>Locked bets 🔒 need more balance.</i>",
        reply_markup=kb_bets(u['balance']),
    )

@bot_router.callback_query(F.data.startswith('bet:'))
async def cb_bet(call: CallbackQuery, session: aiohttp.ClientSession):
    await call.answer()
    raw = call.data.split(':', 1)[1]
    if raw == 'cancel':
        await call.message.edit_text('✅ Cancelled.'); return
    amount = int(raw)
    try:
        u = await bot_api('GET', '/api/user', tg_id=call.from_user.id, session=session)
    except Exception as exc:
        await call.message.edit_text(f'⚠️ {exc}'); return
    if u['balance'] < amount:
        await call.message.edit_text(
            f"⚠️ <b>Insufficient balance</b>\n\n"
            f"You have <b>{u['balance']:,} ETB</b> but need <b>{amount} ETB</b>.\n\n"
            "Use 💰 Deposit to top up.",
        ); return
    await call.message.edit_text(
        f"🎲 <b>{amount} ETB Lobby</b>\n\n"
        f"Prize: <b>90% of the pot</b>\n"
        f"Countdown starts when 2+ players join.\n\n"
        f"Tap below to pick your number and join!",
        reply_markup=kb_webapp(f'🎱 Enter {amount} ETB Game', bet=amount),
    )

# 💰 Deposit ──────────────────────────────────────────────────────────────────
@bot_router.message(F.text == '💰 Deposit')
async def cmd_deposit(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await state.set_state(Deposit.waiting_for_amount)
    await message.answer(
        f"💰 <b>Deposit Funds</b>\n\n"
        f"Balance: <b>{u['balance']:,} ETB</b>\n\n"
        f"Enter amount (min {MIN_DEPOSIT} · max {MAX_DEPOSIT:,} ETB):",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Deposit.waiting_for_amount, F.text)
async def deposit_amount(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Deposit cancelled.', reply_markup=kb_main()); return
    try:
        amt = float(message.text.replace(',', '').strip())
    except ValueError:
        await message.answer('⚠️ Enter a valid number.'); return
    if amt < MIN_DEPOSIT:
        await message.answer(f'⚠️ Minimum deposit: {MIN_DEPOSIT} ETB.'); return
    if amt > MAX_DEPOSIT:
        await message.answer(f'⚠️ Maximum deposit: {MAX_DEPOSIT:,} ETB.'); return
    await state.update_data(deposit_amount=amt)
    await state.set_state(Deposit.waiting_for_sms_ref)
    await message.answer(
        f"✅ Amount: <b>{amt:,.0f} ETB</b>\n\n"
        f"<b>Send payment to:</b>\n{PAYMENT_ACCOUNTS}\n\n"
        f"📌 <b>Reference:</b> <code>BB-{message.from_user.id}</code>\n\n"
        "Reply with your <b>transaction reference / SMS code</b> after paying:",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Deposit.waiting_for_sms_ref, F.text)
async def deposit_ref(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Deposit cancelled.', reply_markup=kb_main()); return
    data = await state.get_data()
    amt  = data['deposit_amount']
    ref  = message.text.strip()
    await state.clear()
    admin_txt = (
        f"📥 <b>Deposit Request</b>\n\nUser: <code>{message.from_user.id}</code>"
        f" (@{message.from_user.username or '—'})\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\nReference: <code>{ref}</code>\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/admindeposit {message.from_user.id} {amt:.0f}</code>"
    )
    for aid in ADMIN_TG_IDS_INT:
        _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    await message.answer(
        f"✅ <b>Deposit request submitted!</b>\n\nAmount: <b>{amt:,.0f} ETB</b>\n"
        "Our team will credit your wallet within <b>30 minutes</b>. 🔔",
        reply_markup=kb_main(),
    )

# 💳 Withdraw ─────────────────────────────────────────────────────────────────
@bot_router.message(F.text == '💳 Withdraw')
async def cmd_withdraw(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    if u['balance'] < MIN_WITHDRAW:
        await message.answer(f"⚠️ Min withdrawal: <b>{MIN_WITHDRAW} ETB</b>.\n"
                             f"Your balance: <b>{u['balance']:,} ETB</b>"); return
    if u['games_played'] < 1:
        await message.answer('⚠️ Play at least one game before withdrawing.'); return
    await state.set_state(Withdrawal.waiting_for_amount)
    await message.answer(
        f"💳 <b>Withdraw Funds</b>\n\nAvailable: <b>{u['balance']:,} ETB</b>\n"
        f"Min: {MIN_WITHDRAW:,} ETB · Processing: up to 24h\n\nEnter amount:",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Withdrawal.waiting_for_amount, F.text)
async def withdraw_amount(message: Message, state: FSMContext, session: aiohttp.ClientSession):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    try:
        amt = float(message.text.replace(',', '').strip())
    except ValueError:
        await message.answer('⚠️ Enter a valid number.'); return
    try:
        u = await bot_api('GET', '/api/user', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    if amt < MIN_WITHDRAW:
        await message.answer(f'⚠️ Minimum: {MIN_WITHDRAW} ETB.'); return
    if amt > u['balance']:
        await message.answer(f"⚠️ Insufficient. Available: {u['balance']:,} ETB."); return
    await state.update_data(withdraw_amount=amt)
    await state.set_state(Withdrawal.waiting_for_phone)
    await message.answer(
        f"📱 Phone number to receive <b>{amt:,.0f} ETB</b>:\n"
        f"Format: <code>09XXXXXXXX</code>",
        reply_markup=kb_cancel(),
    )

@bot_router.message(Withdrawal.waiting_for_phone, F.text)
async def withdraw_phone(message: Message, state: FSMContext):
    if message.text in ('❌ Cancel', '/cancel'):
        await state.clear()
        await message.answer('Withdrawal cancelled.', reply_markup=kb_main()); return
    data  = await state.get_data()
    amt   = data['withdraw_amount']
    phone = message.text.strip()
    await state.clear()
    admin_txt = (
        f"💸 <b>Withdrawal Request</b>\n\nUser: <code>{message.from_user.id}</code>"
        f" (@{message.from_user.username or '—'})\n"
        f"Amount: <b>{amt:,.0f} ETB</b>\nPhone: <code>{phone}</code>\n"
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC\n\n"
        f"✅ Approve: <code>/adminwithdraw {message.from_user.id} {amt:.0f}</code>"
    )
    for aid in ADMIN_TG_IDS_INT:
        _notify_queue.put({'tg_id': aid, 'event': 'custom', 'data': {'message': admin_txt}})
    await message.answer(
        f"✅ <b>Withdrawal request received!</b>\n\nAmount: <b>{amt:,.0f} ETB</b>\n"
        "Processed within <b>24 hours</b>. 🔔",
        reply_markup=kb_main(),
    )

# Admin commands ──────────────────────────────────────────────────────────────
@bot_router.message(Command('admin'))
@admin_guard
async def cmd_admin(message: Message, session: aiohttp.ClientSession):
    try:
        s = await bot_api('GET', '/api/admin/stats', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    await message.answer(
        f"⚙️ <b>Admin Dashboard</b>\n\n"
        f"👥 Users: {s['total_users']}  🎮 Games: {s['total_games']}\n"
        f"🔴 Active: {s['active_games']}  ✅ Finished: {s['finished_games']}\n"
        f"💰 Prizes paid: {s['total_prizes']:,} ETB\n\n"
        f"<b>Commands:</b>\n"
        f"/admindeposit &lt;tg_id&gt; &lt;amount&gt;\n"
        f"/adminwithdraw &lt;tg_id&gt; &lt;amount&gt;\n"
        f"/adminend &lt;game_id&gt;\n"
        f"/adminmsg &lt;tg_id&gt; &lt;text&gt;\n"
        f"/adminbroadcast &lt;text&gt;",
    )

@bot_router.message(Command('admindeposit'))
@admin_guard
async def cmd_adep(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /admindeposit &lt;tg_id&gt; &lt;amount&gt;'); return
    tid, amt = int(parts[1]), float(parts[2])
    try:
        users  = await bot_api('GET', '/api/admin/users', tg_id=message.from_user.id, session=session)
        target = next((u for u in users if u['tg_id'] == str(tid)), None)
        if not target: await message.answer('⚠️ User not found.'); return
        res = await bot_api('POST', '/api/admin/funds', tg_id=message.from_user.id,
                            body={'username': target['username'], 'amount': int(amt)}, session=session)
        await message.answer(f"✅ +{amt:,.0f} ETB → <b>{target['username']}</b>\nBalance: {res['new_balance']:,} ETB")
        _notify_queue.put({'tg_id': tid, 'event': 'deposit_approved', 'data': {'amount': amt, 'new_balance': res['new_balance']}})
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminwithdraw'))
@admin_guard
async def cmd_awdraw(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /adminwithdraw &lt;tg_id&gt; &lt;amount&gt;'); return
    tid, amt = int(parts[1]), float(parts[2])
    try:
        users  = await bot_api('GET', '/api/admin/users', tg_id=message.from_user.id, session=session)
        target = next((u for u in users if u['tg_id'] == str(tid)), None)
        if not target: await message.answer('⚠️ User not found.'); return
        res = await bot_api('POST', '/api/admin/funds', tg_id=message.from_user.id,
                            body={'username': target['username'], 'amount': -int(amt)}, session=session)
        await message.answer(f"✅ Withdrawal {amt:,.0f} ETB confirmed for <b>{target['username']}</b>.\nBalance: {res['new_balance']:,} ETB")
        _notify_queue.put({'tg_id': tid, 'event': 'custom',
                          'data': {'message': f"✅ <b>Withdrawal Processed!</b>\n{amt:,.0f} ETB sent.\nBalance: <b>{res['new_balance']:,} ETB</b>"}})
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminend'))
@admin_guard
async def cmd_aend(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: await message.answer('Usage: /adminend &lt;game_id&gt;'); return
    try:
        res = await bot_api('POST', f'/api/admin/games/{parts[1].strip()}/finish',
                            tg_id=message.from_user.id, session=session)
        await message.answer(f"✅ Game ended. {res.get('refunded', 0)} player(s) refunded.")
    except Exception as exc:
        await message.answer(f'⚠️ {exc}')

@bot_router.message(Command('adminmsg'))
@admin_guard
async def cmd_amsg(message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3 or not parts[1].isdigit():
        await message.answer('Usage: /adminmsg &lt;tg_id&gt; &lt;text&gt;'); return
    _notify_queue.put({'tg_id': int(parts[1]), 'event': 'custom',
                       'data': {'message': f"📢 <b>Admin message:</b>\n\n{parts[2]}"}})
    await message.answer('✅ Message queued.')

@bot_router.message(Command('adminbroadcast'))
@admin_guard
async def cmd_abcast(message: Message, session: aiohttp.ClientSession):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2: await message.answer('Usage: /adminbroadcast &lt;text&gt;'); return
    try:
        users = await bot_api('GET', '/api/admin/users', tg_id=message.from_user.id, session=session)
    except Exception as exc:
        await message.answer(f'⚠️ {exc}'); return
    for u in users:
        _notify_queue.put({'tg_id': int(u['tg_id']), 'event': 'custom',
                           'data': {'message': f"📣 <b>Announcement</b>\n\n{parts[1]}"}})
    await message.answer(f"📣 Broadcast queued for {len(users)} users.")

# ── Notification drain task ───────────────────────────────────────────────────
async def _drain_notify_queue(bot: Bot):
    """
    Runs as an asyncio task inside the bot's event loop.
    Drains the thread-safe queue that Flask route handlers write to.
    """
    event_map = {
        'win':               lambda d: f"🏆 <b>BINGO! You won!</b>\n\n+{d.get('prize',0):,} ETB added to your wallet! 🎱",
        'game_start':        lambda d: '🔔 <b>Your game is starting!</b>\nOpen the app and mark your card!',
        'deposit_approved':  lambda d: f"✅ <b>Deposit Approved!</b>\n\n<b>{d.get('amount',0):,.0f} ETB</b> added.\nBalance: <b>{d.get('new_balance',0):,} ETB</b>",
        'custom':            lambda d: d.get('message', ''),
    }
    while True:
        await asyncio.sleep(0.25)
        while not _notify_queue.empty():
            try:
                item  = _notify_queue.get_nowait()
                tg_id = item['tg_id']
                event = item['event']
                data  = item['data']
                fn    = event_map.get(event)
                if fn:
                    text = fn(data)
                    if text:
                        await bot_notify(bot, tg_id, text)
            except Exception as exc:
                logger.warning(f'[notify_drain] {exc}')

# ── aiohttp middleware to inject shared session ───────────────────────────────
async def _session_middleware(handler, event, data):
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        data['session'] = session
        return await handler(event, data)

# ── Bot main coroutine ────────────────────────────────────────────────────────
async def _bot_main():
    global _bot_username
    if not BOT_TOKEN:
        logger.warning('[Bot] No BOT_TOKEN set — bot will not start.')
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())

    # Inject aiohttp session into every handler via middleware
    dp.update.middleware(_session_middleware)
    dp.include_router(bot_router)

    # Cache bot username
    me = await bot.get_me()
    _bot_username = me.username
    logger.info(f'[Bot] @{_bot_username} started (polling)')

    # Delete any stale webhook so polling works
    await bot.delete_webhook(drop_pending_updates=True)

    # Start notification drain as background task
    asyncio.create_task(_drain_notify_queue(bot))

    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=True,
        )
    finally:
        await bot.session.close()
        logger.info('[Bot] Polling stopped.')

# ── Thread launcher (called once at module load) ──────────────────────────────
_bot_thread_started = False

def start_bot_thread():
    """
    Starts the aiogram bot in a dedicated daemon thread with its own event loop.
    Called once at module load — Gunicorn --workers 1 ensures this happens exactly once.
    """
    global _bot_thread_started
    if _bot_thread_started:
        return
    _bot_thread_started = True

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(_bot_main())
        except Exception as exc:
            logger.error(f'[Bot] Fatal: {exc}')
        finally:
            loop.close()

    t = threading.Thread(target=_run, daemon=True, name='TelegramBot')
    t.start()
    logger.info('[Bot] Daemon thread launched.')

# ── Start the bot when this module is imported by Gunicorn ───────────────────
start_bot_thread()

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT  (python app.py — local dev only)
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    init_db()
    GameEngine()
    port = int(os.environ.get('PORT', 5000))
    print(f'[Benz Bingo] http://0.0.0.0:{port}  DB={DB_TYPE}')
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
