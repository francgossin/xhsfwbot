"""
SQLite database layer for xhsfwbot.

Tables
------
- **users**          Per-Telegram-user preferences (language, default flags, etc.)
- **telegraph_logs** Every Telegraph page ever generated.
- **message_state**  Replaces JSON-per-message files in data/ for action state.
- **daily_stats**    Aggregated daily statistics (pre-computed on insert).

All timestamps are stored as ISO-8601 strings in UTC+8 to match the rest of
the bot's display convention.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

DB_PATH = os.path.join(os.path.dirname(__file__), 'xhsfwbot.db')

_UTC8 = timezone(timedelta(hours=8))

_local = threading.local()


def _get_conn() -> sqlite3.Connection:
    """Return a per-thread reusable connection (SQLite is not thread-safe by default)."""
    conn: sqlite3.Connection | None = getattr(_local, 'conn', None)
    if conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA foreign_keys=ON')
        _local.conn = conn
    return conn


def _now_str() -> str:
    return datetime.now(_UTC8).strftime('%Y-%m-%d %H:%M:%S')


# ── Schema ─────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    tg_user_id     INTEGER PRIMARY KEY,
    tg_username    TEXT    DEFAULT '',
    tg_first_name  TEXT    DEFAULT '',
    tg_last_name   TEXT    DEFAULT '',
    language       TEXT    DEFAULT 'en',
    pref_send_as_file       INTEGER DEFAULT 0,
    pref_include_live       INTEGER DEFAULT 0,
    pref_use_xsec           INTEGER DEFAULT 0,
    pref_keep_original      INTEGER DEFAULT 0,
    created_at     TEXT    NOT NULL,
    updated_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS groups (
    group_id       INTEGER PRIMARY KEY,
    language       TEXT    DEFAULT 'en',
    pref_send_as_file       INTEGER DEFAULT 0,
    pref_include_live       INTEGER DEFAULT 0,
    pref_use_xsec           INTEGER DEFAULT 0,
    pref_keep_original      INTEGER DEFAULT 0,
    created_at     TEXT    NOT NULL,
    updated_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS telegraph_logs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    note_id        TEXT    NOT NULL,
    note_title     TEXT    DEFAULT '',
    note_type      TEXT    DEFAULT '',
    telegraph_url  TEXT    NOT NULL,
    tg_user_id     INTEGER NOT NULL,
    tg_username    TEXT    DEFAULT '',
    tg_first_name  TEXT    DEFAULT '',
    tg_last_name   TEXT    DEFAULT '',
    tg_chat_id     INTEGER NOT NULL,
    tg_message_id  INTEGER DEFAULT 0,
    xsec_token     TEXT    DEFAULT '',
    image_count    INTEGER DEFAULT 0,
    video_count    INTEGER DEFAULT 0,
    created_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS message_state (
    primary_id     TEXT    PRIMARY KEY,
    chat_id        INTEGER NOT NULL,
    data_json      TEXT    NOT NULL,
    created_at     TEXT    NOT NULL,
    updated_at     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS message_aliases (
    alias_id       TEXT    PRIMARY KEY,
    primary_id     TEXT    NOT NULL,
    FOREIGN KEY (primary_id) REFERENCES message_state(primary_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS note_cache (
    note_id             TEXT PRIMARY KEY,
    note_data_json      TEXT,
    comment_list_json   TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_telegraph_created   ON telegraph_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_telegraph_user      ON telegraph_logs(tg_user_id);
CREATE INDEX IF NOT EXISTS idx_telegraph_note      ON telegraph_logs(note_id);
CREATE INDEX IF NOT EXISTS idx_msg_state_chat      ON message_state(chat_id);
CREATE INDEX IF NOT EXISTS idx_msg_alias_primary   ON message_aliases(primary_id);
CREATE INDEX IF NOT EXISTS idx_note_cache_updated  ON note_cache(updated_at);
"""


def init_db() -> None:
    """Create tables if they don't exist. Safe to call multiple times."""
    conn = _get_conn()
    conn.executescript(_SCHEMA)
    # Migrate existing DBs that may lack newer columns
    for sql in [
        "ALTER TABLE telegraph_logs ADD COLUMN tg_last_name TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN pref_keep_original INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


# ── Users ──────────────────────────────────────────────────────────────────────

def get_user(tg_user_id: int) -> dict[str, Any] | None:
    row = _get_conn().execute(
        'SELECT * FROM users WHERE tg_user_id = ?', (tg_user_id,)
    ).fetchone()
    return dict(row) if row else None


def upsert_user(
    tg_user_id: int,
    tg_username: str = '',
    tg_first_name: str = '',
    tg_last_name: str = '',
    **prefs: Any,
) -> None:
    """Create or update a user record. Extra keyword args update preference columns."""
    now = _now_str()
    conn = _get_conn()
    existing = get_user(tg_user_id)
    if existing is None:
        conn.execute(
            '''INSERT INTO users
               (tg_user_id, tg_username, tg_first_name, tg_last_name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)''',
            (tg_user_id, tg_username, tg_first_name, tg_last_name, now, now),
        )
    else:
        conn.execute(
            '''UPDATE users SET tg_username=?, tg_first_name=?, tg_last_name=?, updated_at=?
               WHERE tg_user_id=?''',
            (tg_username or existing['tg_username'],
             tg_first_name or existing['tg_first_name'],
             tg_last_name or existing['tg_last_name'],
             now, tg_user_id),
        )
    # Apply preference overrides
    allowed = {'language', 'pref_send_as_file', 'pref_include_live', 'pref_use_xsec'}
    for k, v in prefs.items():
        if k in allowed:
            conn.execute(f'UPDATE users SET {k}=?, updated_at=? WHERE tg_user_id=?', (v, now, tg_user_id))
    conn.commit()


def set_user_pref(tg_user_id: int, key: str, value: Any) -> bool:
    """Set a single preference. Returns True on success."""
    allowed = {'language', 'pref_send_as_file', 'pref_include_live', 'pref_use_xsec', 'pref_keep_original'}
    if key not in allowed:
        return False
    now = _now_str()
    conn = _get_conn()
    conn.execute(f'UPDATE users SET {key}=?, updated_at=? WHERE tg_user_id=?', (value, now, tg_user_id))
    conn.commit()
    return True


def get_user_lang(tg_user_id: int) -> str:
    """Return user's language code, defaulting to 'en'."""
    user = get_user(tg_user_id)
    return user['language'] if user else 'en'


def get_user_prefs(tg_user_id: int) -> dict[str, Any]:
    """Return user preferences dict with defaults."""
    user = get_user(tg_user_id)
    if user:
        return {
            'language': user['language'],
            'send_as_file': bool(user['pref_send_as_file']),
            'include_live': bool(user['pref_include_live']),
            'use_xsec': bool(user['pref_use_xsec']),
            'keep_original': bool(user.get('pref_keep_original', 0)),
        }
    return {
        'language': 'en',
        'send_as_file': False,
        'include_live': False,
        'use_xsec': False,
        'keep_original': False,
    }


# ── Groups ────────────────────────────────────────────────────────────────────

def upsert_group(group_id: int) -> None:
    """Ensure a group row exists; no-op if already present."""
    now = _now_str()
    conn = _get_conn()
    conn.execute(
        'INSERT OR IGNORE INTO groups (group_id, created_at, updated_at) VALUES (?, ?, ?)',
        (group_id, now, now),
    )
    conn.commit()


def get_group_config(group_id: int) -> dict[str, Any] | None:
    """Return raw group config row as dict, or None if not found."""
    row = _get_conn().execute(
        'SELECT * FROM groups WHERE group_id = ?', (group_id,)
    ).fetchone()
    return dict(row) if row else None


def set_group_pref(group_id: int, key: str, value: Any) -> bool:
    """Set a group preference, creating the row if needed. Returns True on success."""
    allowed = {'language', 'pref_send_as_file', 'pref_include_live', 'pref_use_xsec', 'pref_keep_original'}
    if key not in allowed:
        return False
    now = _now_str()
    conn = _get_conn()
    conn.execute(
        'INSERT OR IGNORE INTO groups (group_id, created_at, updated_at) VALUES (?, ?, ?)',
        (group_id, now, now),
    )
    conn.execute(f'UPDATE groups SET {key}=?, updated_at=? WHERE group_id=?', (value, now, group_id))
    conn.commit()
    return True


def get_group_lang(group_id: int) -> str:
    """Return group's language code, defaulting to 'en'."""
    cfg = get_group_config(group_id)
    return cfg['language'] if cfg else 'en'


def get_group_prefs(group_id: int) -> dict[str, Any]:
    """Return group preferences dict with defaults."""
    cfg = get_group_config(group_id)
    if cfg:
        return {
            'language': cfg['language'],
            'send_as_file': bool(cfg['pref_send_as_file']),
            'include_live': bool(cfg['pref_include_live']),
            'use_xsec': bool(cfg['pref_use_xsec']),
            'keep_original': bool(cfg['pref_keep_original']),
        }
    return {
        'language': 'en',
        'send_as_file': False,
        'include_live': False,
        'use_xsec': False,
        'keep_original': False,
    }


# ── Telegraph Logs ─────────────────────────────────────────────────────────────

def log_telegraph(
    note_id: str,
    note_title: str,
    note_type: str,
    telegraph_url: str,
    tg_user_id: int,
    tg_username: str,
    tg_first_name: str,
    tg_last_name: str,
    tg_chat_id: int,
    tg_message_id: int = 0,
    xsec_token: str = '',
    image_count: int = 0,
    video_count: int = 0,
) -> int:
    """Insert a telegraph log entry. Returns the row id."""
    now = _now_str()
    conn = _get_conn()
    cur = conn.execute(
        '''INSERT INTO telegraph_logs
           (note_id, note_title, note_type, telegraph_url,
            tg_user_id, tg_username, tg_first_name, tg_last_name, tg_chat_id, tg_message_id,
            xsec_token, image_count, video_count, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        (note_id, note_title, note_type, telegraph_url,
         tg_user_id, tg_username, tg_first_name, tg_last_name, tg_chat_id, tg_message_id,
         xsec_token, image_count, video_count, now),
    )
    conn.commit()
    return cur.lastrowid  # type: ignore[return-value]


def get_telegraphs_for_date(date_str: str) -> list[dict[str, Any]]:
    """Return all telegraph logs for a given date (YYYY-MM-DD)."""
    rows = _get_conn().execute(
        "SELECT * FROM telegraph_logs WHERE created_at LIKE ? ORDER BY created_at",
        (f'{date_str}%',),
    ).fetchall()
    return [dict(r) for r in rows]


def get_telegraphs_for_range(start: str, end: str) -> list[dict[str, Any]]:
    """Return telegraph logs between two dates (inclusive, YYYY-MM-DD)."""
    rows = _get_conn().execute(
        "SELECT * FROM telegraph_logs WHERE created_at >= ? AND created_at < ? ORDER BY created_at",
        (f'{start} 00:00:00', f'{end} 23:59:59'),
    ).fetchall()
    return [dict(r) for r in rows]


def get_telegraphs_for_user(tg_user_id: int) -> list[dict[str, Any]]:
    rows = _get_conn().execute(
        "SELECT * FROM telegraph_logs WHERE tg_user_id = ? ORDER BY created_at DESC",
        (tg_user_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_unique_users_for_date(date_str: str) -> list[dict[str, Any]]:
    """Distinct users who generated telegraphs on a given date."""
    rows = _get_conn().execute(
        """SELECT DISTINCT tg_user_id, tg_username, tg_first_name, tg_last_name
           FROM telegraph_logs WHERE created_at LIKE ?""",
        (f'{date_str}%',),
    ).fetchall()
    return [dict(r) for r in rows]


def get_daily_summary(date_str: str) -> dict[str, Any]:
    """Get aggregated stats for a date."""
    conn = _get_conn()
    row = conn.execute(
        """SELECT COUNT(*) as total_telegraphs,
                  COUNT(DISTINCT tg_user_id) as unique_users,
                  SUM(image_count) as total_images,
                  SUM(video_count) as total_videos
           FROM telegraph_logs WHERE created_at LIKE ?""",
        (f'{date_str}%',),
    ).fetchone()
    return dict(row) if row else {
        'total_telegraphs': 0, 'unique_users': 0,
        'total_images': 0, 'total_videos': 0,
    }


# ── Message State (replaces JSON files) ───────────────────────────────────────

def save_message_state(primary_id: str, chat_id: int, data: dict[str, Any]) -> None:
    """Insert or replace message state."""
    now = _now_str()
    conn = _get_conn()
    conn.execute(
        '''INSERT OR REPLACE INTO message_state (primary_id, chat_id, data_json, created_at, updated_at)
           VALUES (?, ?, ?, COALESCE((SELECT created_at FROM message_state WHERE primary_id=?), ?), ?)''',
        (primary_id, chat_id, json.dumps(data, ensure_ascii=False), primary_id, now, now),
    )
    conn.commit()


def save_message_alias(alias_id: str, primary_id: str) -> None:
    conn = _get_conn()
    conn.execute(
        'INSERT OR REPLACE INTO message_aliases (alias_id, primary_id) VALUES (?, ?)',
        (alias_id, primary_id),
    )
    conn.commit()


def load_message_state(lookup_id: str) -> tuple[dict[str, Any], str] | None:
    """Load message state by primary_id or alias. Returns (data_dict, primary_id) or None."""
    conn = _get_conn()
    # Try direct
    row = conn.execute(
        'SELECT primary_id, data_json FROM message_state WHERE primary_id = ?', (lookup_id,)
    ).fetchone()
    if row:
        return json.loads(row['data_json']), row['primary_id']
    # Try alias
    alias_row = conn.execute(
        'SELECT primary_id FROM message_aliases WHERE alias_id = ?', (lookup_id,)
    ).fetchone()
    if alias_row:
        row = conn.execute(
            'SELECT primary_id, data_json FROM message_state WHERE primary_id = ?',
            (alias_row['primary_id'],),
        ).fetchone()
        if row:
            return json.loads(row['data_json']), row['primary_id']
    return None


def update_message_state(primary_id: str, data: dict[str, Any]) -> None:
    """Update data_json for an existing message state."""
    now = _now_str()
    conn = _get_conn()
    conn.execute(
        'UPDATE message_state SET data_json=?, updated_at=? WHERE primary_id=?',
        (json.dumps(data, ensure_ascii=False), now, primary_id),
    )
    conn.commit()


def get_message_file_path(primary_id: str) -> str:
    """Return a virtual 'file path' for compatibility. Not actually used for file I/O."""
    return f'db:{primary_id}'


# ── Report queries ─────────────────────────────────────────────────────────────

def generate_report_csv(date_str: str) -> str:
    """Generate CSV content for a single day report."""
    telegraphs = get_telegraphs_for_date(date_str)
    summary = get_daily_summary(date_str)
    users = get_unique_users_for_date(date_str)

    lines: list[str] = []
    lines.append(f'Daily Report: {date_str}')
    lines.append(f'Total Telegraphs: {summary["total_telegraphs"]}')
    lines.append(f'Unique Users: {summary["unique_users"]}')
    lines.append(f'Total Images: {summary["total_images"] or 0}')
    lines.append(f'Total Videos: {summary["total_videos"] or 0}')
    lines.append('')
    lines.append('Users:')
    for u in users:
        full_name = ' '.join(filter(None, [u['tg_first_name'], u.get('tg_last_name', '')]))
        lines.append(f'  {u["tg_user_id"]}, @{u["tg_username"]}, {full_name}')
    lines.append('')
    lines.append('No,Time,NoteID,Title,Type,Images,Videos,TelegraphURL,UserID,Username,Name')
    for i, t in enumerate(telegraphs, 1):
        title = (t['note_title'] or '').replace(',', ' ')
        full_name = ' '.join(filter(None, [t['tg_first_name'], t.get('tg_last_name', '')])).replace(',', ' ')
        lines.append(
            f'{i},{t["created_at"]},{t["note_id"]},{title},{t["note_type"]},'
            f'{t["image_count"]},{t["video_count"]},{t["telegraph_url"]},'
            f'{t["tg_user_id"]},@{t["tg_username"]},{full_name}'
        )
    return '\n'.join(lines)


def generate_report_csv_range(start: str, end: str) -> str:
    """Generate CSV content for a date range report."""
    telegraphs = get_telegraphs_for_range(start, end)
    lines: list[str] = []
    lines.append(f'Report: {start} to {end}')
    lines.append(f'Total Entries: {len(telegraphs)}')

    # Per-day summary
    day_counts: dict[str, int] = {}
    for t in telegraphs:
        day = t['created_at'][:10]
        day_counts[day] = day_counts.get(day, 0) + 1
    lines.append('')
    lines.append('Per-day Counts:')
    for day, count in sorted(day_counts.items()):
        lines.append(f'  {day}: {count}')

    lines.append('')
    lines.append('No,Time,NoteID,Title,Type,Images,Videos,TelegraphURL,UserID,Username,Name')
    for i, t in enumerate(telegraphs, 1):
        title = (t['note_title'] or '').replace(',', ' ')
        full_name = ' '.join(filter(None, [t['tg_first_name'], t.get('tg_last_name', '')])).replace(',', ' ')
        lines.append(
            f'{i},{t["created_at"]},{t["note_id"]},{title},{t["note_type"]},'
            f'{t["image_count"]},{t["video_count"]},{t["telegraph_url"]},'
            f'{t["tg_user_id"]},@{t["tg_username"]},{full_name}'
        )
    return '\n'.join(lines)


def generate_report_csv_user(tg_user_id: int) -> str:
    """Generate CSV content for a specific user's history."""
    telegraphs = get_telegraphs_for_user(tg_user_id)
    user = get_user(tg_user_id)
    user_full_name = ' '.join(filter(None, [user['tg_first_name'], user.get('tg_last_name', '')])) if user else str(tg_user_id)
    username = user['tg_username'] if user else ''

    lines: list[str] = []
    lines.append(f'User Report: {user_full_name} (@{username}) [{tg_user_id}]')
    lines.append(f'Total Telegraphs: {len(telegraphs)}')
    lines.append('')
    lines.append('No,Time,NoteID,Title,Type,Images,Videos,TelegraphURL')
    for i, t in enumerate(telegraphs, 1):
        title = (t['note_title'] or '').replace(',', ' ')
        lines.append(
            f'{i},{t["created_at"]},{t["note_id"]},{title},{t["note_type"]},'
            f'{t["image_count"]},{t["video_count"]},{t["telegraph_url"]}'
        )
    return '\n'.join(lines)


# ── Note cache ─────────────────────────────────────────────────────────────────

def save_note_cache(
    note_id: str,
    note_data: dict[str, Any] | None = None,
    comment_list_data: dict[str, Any] | None = None,
) -> None:
    """Insert or update cached note data and/or comment list for a note ID."""
    now = _now_str()
    conn = _get_conn()
    existing = conn.execute(
        'SELECT note_id FROM note_cache WHERE note_id = ?', (note_id,)
    ).fetchone()
    if existing is None:
        conn.execute(
            '''INSERT INTO note_cache
               (note_id, note_data_json, comment_list_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)''',
            (
                note_id,
                json.dumps(note_data, ensure_ascii=False) if note_data is not None else None,
                json.dumps(comment_list_data, ensure_ascii=False) if comment_list_data is not None else None,
                now, now,
            ),
        )
    else:
        parts: list[str] = ['updated_at = ?']
        params: list[Any] = [now]
        if note_data is not None:
            parts.append('note_data_json = ?')
            params.append(json.dumps(note_data, ensure_ascii=False))
        if comment_list_data is not None:
            parts.append('comment_list_json = ?')
            params.append(json.dumps(comment_list_data, ensure_ascii=False))
        params.append(note_id)
        conn.execute(
            f'UPDATE note_cache SET {", ".join(parts)} WHERE note_id = ?',
            params,
        )
    conn.commit()


def load_note_cache(note_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Load cached note data and comment list for a note ID.

    Returns (note_data, comment_list_data) — either can be None if not cached.
    """
    row = _get_conn().execute(
        'SELECT note_data_json, comment_list_json FROM note_cache WHERE note_id = ?',
        (note_id,),
    ).fetchone()
    if row is None:
        return None, None
    nd = json.loads(row['note_data_json']) if row['note_data_json'] else None
    cl = json.loads(row['comment_list_json']) if row['comment_list_json'] else None
    return nd, cl


# ── Migration helper ───────────────────────────────────────────────────────────

def migrate_json_files(data_dir: str = 'data') -> int:
    """Migrate existing JSON message-state and note-cache files into SQLite.

    Successfully migrated files are deleted from disk to avoid re-processing
    on subsequent startups.  Returns count of files migrated.
    """
    if not os.path.isdir(data_dir):
        return 0
    count = 0
    conn = _get_conn()
    _files_to_delete: list[str] = []

    # First pass: collect note_data / comment_list_data cache files
    _note_cache: dict[str, dict[str, Any]] = {}  # note_id -> {note_data, comment_list}
    _note_files: dict[str, list[str]] = {}  # note_id -> list of file paths
    for fname in os.listdir(data_dir):
        if not fname.endswith('.json'):
            continue
        if fname.startswith('note_data-'):
            note_id = fname[len('note_data-'):-5]  # strip prefix and .json
            fpath = os.path.join(data_dir, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                _note_cache.setdefault(note_id, {})['note_data'] = data
                _note_files.setdefault(note_id, []).append(fpath)
            except Exception:
                continue
        elif fname.startswith('comment_list_data-'):
            note_id = fname[len('comment_list_data-'):-5]
            fpath = os.path.join(data_dir, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                _note_cache.setdefault(note_id, {})['comment_list'] = data
                _note_files.setdefault(note_id, []).append(fpath)
            except Exception:
                continue

    for note_id, cached in _note_cache.items():
        try:
            save_note_cache(
                note_id,
                note_data=cached.get('note_data'),
                comment_list_data=cached.get('comment_list'),
            )
            count += 1
            _files_to_delete.extend(_note_files.get(note_id, []))
        except Exception:
            pass

    # Second pass: message-state and alias files
    for fname in os.listdir(data_dir):
        if not fname.endswith('.json'):
            continue
        if fname.startswith('note_data-') or fname.startswith('comment_list_data-'):
            continue  # already handled above
        fpath = os.path.join(data_dir, fname)
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            continue
        key = fname[:-5]  # strip .json
        if 'ref' in data:
            # It's an alias
            try:
                save_message_alias(key, data['ref'])
                count += 1
                _files_to_delete.append(fpath)
            except Exception:
                pass
        elif '_primary_id' in data:
            # It's a primary state file
            parts = key.split('.', 1)
            chat_id = int(parts[0]) if len(parts) == 2 and parts[0].lstrip('-').isdigit() else 0
            try:
                save_message_state(key, chat_id, data)
                count += 1
                _files_to_delete.append(fpath)
            except Exception:
                pass
    conn.commit()

    # Delete migrated files
    deleted = 0
    for fpath in _files_to_delete:
        try:
            os.remove(fpath)
            deleted += 1
        except Exception:
            pass
    if deleted:
        # Log is handled by caller; just ensure consistency
        pass

    return count
