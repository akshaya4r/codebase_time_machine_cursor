from __future__ import annotations
from pathlib import Path
from datetime import datetime
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from .db import connect


def visualize(db_path: Path, outdir: Path) -> list[Path]:
	conn = connect(db_path)
	out = Path(outdir)
	out.mkdir(parents=True, exist_ok=True)

	# Ownership by author (top 12)
	rows = conn.execute(
		"""
		SELECT author_email, SUM(commits) as commits
		FROM ownership
		GROUP BY author_email
		ORDER BY commits DESC
		LIMIT 12
		"""
	).fetchall()
	if rows:
		labels = [r["author_email"] or "unknown" for r in rows]
		values = [r["commits"] for r in rows]
		plt.figure(figsize=(8, 8))
		plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
		plt.title("Ownership by commits")
		p1 = out / "ownership_pie.png"
		plt.savefig(p1, bbox_inches="tight")
		plt.close()
	else:
		p1 = None

	# Churn over time (weekly sum of additions+deletions)
	rows = conn.execute(
		"""
		SELECT (authored_date/604800)*604800 AS week_start,
		       SUM(COALESCE(cf.additions,0)+COALESCE(cf.deletions,0)) AS churn
		FROM commits c
		JOIN commit_files cf ON cf.commit_id = c.id
		GROUP BY week_start
		ORDER BY week_start ASC
		"""
	).fetchall()
	if rows:
		x = [datetime.utcfromtimestamp(r["week_start"]) for r in rows]
		y = [r["churn"] for r in rows]
		plt.figure(figsize=(10, 4))
		plt.plot(x, y, marker="o")
		plt.title("Churn over time (weekly)")
		plt.xlabel("Week")
		plt.ylabel("Lines changed")
		plt.grid(True, alpha=0.3)
		p2 = out / "churn_weekly.png"
		plt.savefig(p2, bbox_inches="tight")
		plt.close()
	else:
		p2 = None

	# Complexity trend: average CCN per commit (if available)
	rows = conn.execute(
		"""
		SELECT c.authored_date as ts,
		       AVG(CASE WHEN coalesce(x.functions,0) > 0 THEN 1.0*x.ccn / x.functions ELSE NULL END) as avg_ccn
		FROM commits c
		JOIN complexity x ON x.commit_id = c.id
		GROUP BY c.id
		ORDER BY ts ASC
		"""
	).fetchall()
	if rows:
		x = [datetime.utcfromtimestamp(r["ts"]) for r in rows]
		y = [r["avg_ccn"] for r in rows]
		plt.figure(figsize=(10, 4))
		plt.plot(x, y, marker=".")
		plt.title("Average cyclomatic complexity per commit")
		plt.xlabel("Time")
		plt.ylabel("Avg CCN")
		plt.grid(True, alpha=0.3)
		p3 = out / "complexity_avg.png"
		plt.savefig(p3, bbox_inches="tight")
		plt.close()
	else:
		p3 = None

	# HTML report
	html_path = out / "report.html"
	html = [
		"<html><head><meta charset='utf-8'><title>Codebase Time Machine Report</title></head><body>",
		"<h1>Codebase Time Machine Report</h1>",
	]
	if p1:
		html.append(f"<h2>Ownership</h2><img src='{p1.name}' style='max-width:100%'>")
	if p2:
		html.append(f"<h2>Churn</h2><img src='{p2.name}' style='max-width:100%'>")
	if p3:
		html.append(f"<h2>Complexity</h2><img src='{p3.name}' style='max-width:100%'>")
	html.append("</body></html>")
	html_path.write_text("\n".join(html), encoding="utf-8")

	return [p for p in [p1, p2, p3, html_path] if p]