from __future__ import annotations
from pathlib import Path
import sqlite3

from .db import connect


def compute_ownership(db_path: Path) -> None:
	conn = connect(db_path)
	# Rebuild ownership table
	conn.execute("DELETE FROM ownership")
	conn.commit()
	conn.execute(
		"""
		INSERT INTO ownership(file_id, author_email, commits, lines_added, lines_deleted, first_commit, last_commit)
		SELECT cf.file_id,
		       c.author_email,
		       COUNT(DISTINCT cf.commit_id) as commits,
		       SUM(COALESCE(cf.additions,0)) as lines_added,
		       SUM(COALESCE(cf.deletions,0)) as lines_deleted,
		       MIN(c.authored_date) as first_commit,
		       MAX(c.authored_date) as last_commit
		FROM commit_files cf
		JOIN commits c ON c.id = cf.commit_id
		GROUP BY cf.file_id, c.author_email
		"""
	)
	conn.commit()


def ensure_indexes(db_path: Path) -> None:
	conn = connect(db_path)
	conn.executescript(
		"""
		CREATE INDEX IF NOT EXISTS idx_commits_date ON commits(authored_date);
		CREATE INDEX IF NOT EXISTS idx_features_commit ON features(commit_id);
		"""
	)
	conn.commit()