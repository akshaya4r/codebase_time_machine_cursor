import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, Optional


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS repo_meta (
	key TEXT PRIMARY KEY,
	value TEXT
);

CREATE TABLE IF NOT EXISTS commits (
	id TEXT PRIMARY KEY,
	author_name TEXT,
	author_email TEXT,
	authored_date INTEGER,
	message TEXT
);

CREATE TABLE IF NOT EXISTS files (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	path TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS commit_files (
	commit_id TEXT,
	file_id INTEGER,
	additions INTEGER,
	deletions INTEGER,
	change_type TEXT,
	old_path TEXT,
	new_path TEXT,
	is_binary INTEGER DEFAULT 0,
	FOREIGN KEY(commit_id) REFERENCES commits(id) ON DELETE CASCADE,
	FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_commit_files_commit ON commit_files(commit_id);
CREATE INDEX IF NOT EXISTS idx_commit_files_file ON commit_files(file_id);

CREATE TABLE IF NOT EXISTS ownership (
	file_id INTEGER,
	author_email TEXT,
	commits INTEGER,
	lines_added INTEGER,
	lines_deleted INTEGER,
	first_commit INTEGER,
	last_commit INTEGER,
	PRIMARY KEY (file_id, author_email),
	FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS complexity (
	file_id INTEGER,
	commit_id TEXT,
	nloc INTEGER,
	ccn INTEGER,
	functions INTEGER,
	PRIMARY KEY (file_id, commit_id),
	FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
	FOREIGN KEY(commit_id) REFERENCES commits(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS features (
	commit_id TEXT,
	type TEXT,
	reference TEXT,
	FOREIGN KEY(commit_id) REFERENCES commits(id) ON DELETE CASCADE
);
"""


def connect(db_path: Path) -> sqlite3.Connection:
	conn = sqlite3.connect(str(db_path))
	conn.row_factory = sqlite3.Row
	return conn


def init_db(conn: sqlite3.Connection) -> None:
	conn.executescript(SCHEMA_SQL)
	conn.commit()


def upsert_file(conn: sqlite3.Connection, path: str) -> int:
	cur = conn.execute("INSERT OR IGNORE INTO files(path) VALUES (?)", (path,))
	if cur.lastrowid:
		return int(cur.lastrowid)
	row = conn.execute("SELECT id FROM files WHERE path=?", (path,)).fetchone()
	return int(row[0])


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
	conn.execute("INSERT INTO repo_meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
	conn.commit()


def get_meta(conn: sqlite3.Connection, key: str) -> Optional[str]:
	row = conn.execute("SELECT value FROM repo_meta WHERE key=?", (key,)).fetchone()
	return row[0] if row else None


def bulk_insert_commit_files(conn: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
	conn.executemany(
		"""
		INSERT INTO commit_files(commit_id, file_id, additions, deletions, change_type, old_path, new_path, is_binary)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		""",
		rows,
	)


def bulk_insert_commits(conn: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
	conn.executemany(
		"""
		INSERT OR IGNORE INTO commits(id, author_name, author_email, authored_date, message)
		VALUES (?, ?, ?, ?, ?)
		""",
		rows,
	)


def bulk_insert_features(conn: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
	conn.executemany(
		"INSERT INTO features(commit_id, type, reference) VALUES (?, ?, ?)",
		rows,
	)


def bulk_insert_complexity(conn: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
	conn.executemany(
		"INSERT OR REPLACE INTO complexity(file_id, commit_id, nloc, ccn, functions) VALUES (?, ?, ?, ?, ?)",
		rows,
	)