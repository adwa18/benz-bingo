"""
Benz Bingo — Flask Backend
===========================
Database: auto-detects from DATABASE_URL environment variable.
  • If DATABASE_URL starts with postgres → uses psycopg2 (PostgreSQL)
  • Otherwise                           → uses sqlite3  (local dev)

Environment variables:
  DATABASE_URL          — PostgreSQL connection string (set by Render automatically)
                          e.g. postgresql://user:pass@host/dbname
  TELEGRAM_BOT_TOKEN    — from @BotFather
  ADMIN_TG_IDS          — comma-separated Telegram user IDs
  WELCOME_BONUS         — ETB given to new users (default: 100)
  PORT                  — HTTP port (default: 5000)

Run locally:   python app.py
Run on Render: gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 4 app:app
"""

import json
import hashlib
import hmac
import os
import random
import threading
import time
import uuid
from flask import Flask, request, jsonify, g, send_from_directory
from flask_cors import CORS

# ─── CONFIG ───────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')
CORS(app, resources={r"/api/*": {"origins": "*"}})

BOT_TOKEN      = os.environ.get('TELEGRAM_BOT_TOKEN', os.environ.get('BOT_TOKEN', ''))
ADMIN_TG_IDS   = set(os.environ.get('ADMIN_TG_IDS', '').split(','))
WELCOME_BONUS  = int(os.environ.get('WELCOME_BONUS', 100))
HOUSE_CUT      = 0.10
COUNTDOWN_SECS = 120
CALL_INTERVAL  = 5
BET_AMOUNTS    = [10, 50, 100, 200]

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

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    init_db()
    GameEngine()
    port = int(os.environ.get('PORT', 5000))
    print(f'[Benz Bingo] http://0.0.0.0:{port}  DB={DB_TYPE}')
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
