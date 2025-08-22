from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Iterable, Optional
import re

from git import Repo
from git.objects.commit import Commit
from tqdm import tqdm

from .db import connect, init_db, upsert_file, bulk_insert_commit_files, bulk_insert_commits, bulk_insert_features, bulk_insert_complexity, set_meta

try:
	from lizard import analyze_file
	def analyze_code_string(file_path: str, code: str):
		return analyze_file.analyze_source_code(file_path, code)
except Exception:  # lizard optional
	analyze_file = None
	def analyze_code_string(file_path: str, code: str):
		return None


FEATURE_PATTERNS = [
	re.compile(r"fixe?s?\s+#(?P<num>\d+)", re.I),
	re.compile(r"close[sd]?\s+#(?P<num>\d+)", re.I),
	re.compile(r"(?P<jira>[A-Z][A-Z0-9]+-\d+)")
]

WHY_KEYWORDS = ["why", "because", "reason", "motivation", "rfc", "design", "introduc", "decision"]


class GitIndexer:
	def __init__(self, repo_dir: Path, db_path: Path):
		self.repo_dir = Path(repo_dir)
		self.db_path = Path(db_path)
		self.repo: Optional[Repo] = None

	def clone_if_needed(self, repo_url: str) -> None:
		if self.repo_dir.exists() and any(self.repo_dir.iterdir()):
			self.repo = Repo(str(self.repo_dir))
			return
		self.repo_dir.mkdir(parents=True, exist_ok=True)
		self.repo = Repo.clone_from(repo_url, str(self.repo_dir))

	def open_repo(self) -> None:
		self.repo = Repo(str(self.repo_dir))

	def index(self) -> None:
		assert self.repo is not None
		conn = connect(self.db_path)
		init_db(conn)
		set_meta(conn, "repo_dir", str(self.repo_dir))

		commits: List[Commit] = list(self.repo.iter_commits("--all"))
		rows_commits: List[Tuple] = []
		rows_commit_files: List[Tuple] = []
		rows_features: List[Tuple] = []
		rows_complex: List[Tuple] = []

		for c in tqdm(commits, desc="Indexing commits"):
			rows_commits.append((c.hexsha, c.author.name, c.author.email, c.authored_date, c.message))
		conn = connect(self.db_path)
		bulk_insert_commits(conn, rows_commits)
		conn.commit()

		for c in tqdm(commits, desc="Indexing diffs"):
			parent = c.parents[0] if c.parents else None
			# Use diff to get change types and paths
			diffs = c.diff(parent, create_patch=False, R=False)
			for d in diffs:
				change_type = d.change_type.upper()  # 'A','M','D','R','T'
				old_path = d.a_path
				new_path = d.b_path
				path_eff = new_path if change_type != 'D' else old_path

				# Stats for additions/deletions from commit.stats if available
				stats = c.stats.files.get(path_eff, {"insertions": 0, "deletions": 0})
				additions = int(stats.get("insertions", 0))
				deletions = int(stats.get("deletions", 0))

				file_id = upsert_file(conn, path_eff)
				is_binary = 1 if d.b_blob is not None and d.b_blob.is_binary else 0
				rows_commit_files.append((c.hexsha, file_id, additions, deletions, change_type, old_path, new_path, is_binary))

				# Complexity for non-binary files on this commit for the effective path
				if analyze_code_string is not None and change_type != 'D' and d.b_blob is not None and not d.b_blob.is_binary:
					try:
						code_bytes = d.b_blob.data_stream.read()
						code = code_bytes.decode(errors='ignore')
						result = analyze_code_string(path_eff, code)
						if result:
							nloc = int(getattr(result, 'nloc', 0) or 0)
							functions = len(getattr(result, 'function_list', []) or [])
							ccn_total = sum(getattr(f, 'cyclomatic_complexity', 0) or 0 for f in getattr(result, 'function_list', []) or [])
							rows_complex.append((file_id, c.hexsha, nloc, ccn_total, functions))
					except Exception:
						pass

			# Feature references from message
			for pat in FEATURE_PATTERNS:
				for m in pat.finditer(c.message or ""):
					ref = m.groupdict().get("num") or m.groupdict().get("jira") or m.group(0)
					rows_features.append((c.hexsha, pat.pattern, str(ref)))

			# Why markers
			if any(k in (c.message or "").lower() for k in WHY_KEYWORDS):
				rows_features.append((c.hexsha, "why_marker", "1"))

			# Flush periodically to keep memory in check
			if len(rows_commit_files) >= 1000:
				bulk_insert_commit_files(conn, rows_commit_files)
				rows_commit_files.clear()
				conn.commit()
			if len(rows_complex) >= 500:
				bulk_insert_complexity(conn, rows_complex)
				rows_complex.clear()
				conn.commit()
			if len(rows_features) >= 1000:
				bulk_insert_features(conn, rows_features)
				rows_features.clear()
				conn.commit()

		# Final flush
		if rows_commit_files:
			bulk_insert_commit_files(conn, rows_commit_files)
			conn.commit()
		if rows_complex:
			bulk_insert_complexity(conn, rows_complex)
			conn.commit()
		if rows_features:
			bulk_insert_features(conn, rows_features)
			conn.commit()