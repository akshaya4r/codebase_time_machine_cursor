from __future__ import annotations
import typer
from pathlib import Path
from rich.console import Console

from .git_indexer import GitIndexer
from .analysis import compute_ownership, ensure_indexes
from .qa import ask
from .visualize import visualize
from .db import connect, init_db, get_meta, set_meta

app = typer.Typer(add_completion=False, help="Codebase Time Machine CLI")
console = Console()


@app.command()
def init(
	repo_url: str = typer.Option(..., help="Git repository URL to clone"),
	workdir: Path = typer.Option(..., path_type=Path, help="Directory to clone the repository into"),
	db: Path = typer.Option(..., path_type=Path, help="SQLite DB file path"),
):
	"""Clone a repo, index its history, and compute analyses."""
	console.rule("Clone & Index")
	indexer = GitIndexer(workdir, db)
	indexer.clone_if_needed(repo_url)
	indexer.index()

	console.rule("Analyze")
	compute_ownership(db)
	ensure_indexes(db)
	set_meta(connect(db), "db_path", str(db))
	console.print("Done")


@app.command()
def index(
	repo_dir: Path = typer.Option(..., path_type=Path, help="Existing local repository directory"),
	db: Path = typer.Option(..., path_type=Path, help="SQLite DB file path"),
):
	"""Index (or re-index) the repository's full history."""
	indexer = GitIndexer(repo_dir, db)
	indexer.open_repo()
	indexer.index()
	compute_ownership(db)
	ensure_indexes(db)
	console.print("Indexing complete")


@app.command()
def query(
	db: Path = typer.Option(..., path_type=Path, help="SQLite DB file path"),
	question: str = typer.Argument(..., help="Natural language question about the repo's evolution"),
):
	"""Answer questions about code evolution and rationale from the indexed history."""
	answer = ask(db, question)
	console.print(answer)


@app.command(name="visualize")
def visualize_cmd(
	db: Path = typer.Option(..., path_type=Path, help="SQLite DB file path"),
	outdir: Path = typer.Option(..., path_type=Path, help="Directory to write visualization files"),
):
	"""Generate visualizations and a simple HTML report."""
	paths = visualize(db, outdir)
	for p in paths:
		console.print(f"Wrote: {p}")


if __name__ == "__main__":
	app()