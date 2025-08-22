from __future__ import annotations
from pathlib import Path
import sqlite3
import re
from datetime import datetime

from .db import connect


DEFAULT_LIMIT = 20


def _rows_to_str(rows) -> str:
	lines = []
	for r in rows:
		date = datetime.utcfromtimestamp(r["authored_date"]).strftime("%Y-%m-%d") if r["authored_date"] else ""
		lines.append(f"{date} | {r['id'][:10]} | {r['author_name']} <{r['author_email']}> | {r['message'].splitlines()[0] if r['message'] else ''}")
	return "\n".join(lines)


auth_keywords = ["auth", "login", "signin", "jwt", "oauth", "sso", "session", "password", "authorization", "authentication"]


def ask(db_path: Path, question: str) -> str:
	conn = connect(db_path)
	q = question.lower()

	if any(k in q for k in auth_keywords):
		return evolution(conn, auth_keywords, title="Authentication / Authorization evolution")

	if "why" in q or "reason" in q or "motivation" in q or "pattern" in q:
		return why_search(conn, q)

	# Fallback: keyword search in messages and file paths
	keywords = [w for w in re.split(r"[^a-z0-9]+", q) if len(w) >= 3]
	if not keywords:
		return "No searchable keywords detected. Try a more specific query."
	pattern = "%" + "%".join(keywords) + "%"
	rows = conn.execute(
		"""
		SELECT c.* FROM commits c
		WHERE c.message LIKE ?
		ORDER BY c.authored_date ASC
		LIMIT ?
		""",
		(pattern, DEFAULT_LIMIT),
	).fetchall()
	if rows:
		return _rows_to_str(rows)

	rows = conn.execute(
		"""
		SELECT DISTINCT c.* FROM commits c
		JOIN commit_files cf ON cf.commit_id = c.id
		JOIN files f ON f.id = cf.file_id
		WHERE f.path LIKE ?
		ORDER BY c.authored_date ASC
		LIMIT ?
		""",
		(pattern, DEFAULT_LIMIT),
	).fetchall()
	if rows:
		return _rows_to_str(rows)
	return "No relevant commits found."


def evolution(conn: sqlite3.Connection, keywords: list[str], title: str = "Evolution") -> str:
	like_clauses = " OR ".join(["f.path LIKE ?"] * len(keywords))
	params = [f"%{k}%" for k in keywords]
	rows = conn.execute(
		f"""
		SELECT DISTINCT c.* FROM commits c
		JOIN commit_files cf ON cf.commit_id = c.id
		JOIN files f ON f.id = cf.file_id
		WHERE {like_clauses}
		ORDER BY c.authored_date ASC
		LIMIT 200
		""",
		params,
	).fetchall()
	if not rows:
		return f"{title}: no related commits found."
	return f"{title}:\n" + _rows_to_str(rows)


def why_search(conn: sqlite3.Connection, query: str) -> str:
	keywords = ["why", "because", "reason", "motivation", "introduc", "refactor", "rfc", "design", "pattern"]
	like = "%" + "%".join([w for w in keywords if w in query.lower()] or ["why"]) + "%"
	rows = conn.execute(
		"""
		SELECT * FROM commits
		WHERE lower(message) LIKE ?
		ORDER BY authored_date ASC
		LIMIT 100
		""",
		(like,),
	).fetchall()
	if not rows:
		return "No explicit rationale found in commit messages. Try different keywords or search an RFC directory."
	return _rows_to_str(rows)