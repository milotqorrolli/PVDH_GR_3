"""Preprocessing utilities.

This module provides a small CSV loader to start the data preprocessing
pipeline. The function is intentionally simple: it locates files under
`data/raw`, uses pandas to read them, and exposes a few convenient
parameters (nrows, parse_dates, dtype, usecols).
"""
from pathlib import Path
from typing import Optional, Sequence, Dict

import pandas as pd


RAW_DIR = Path("data") / "raw"


def load_csv(
	filename: str,
	nrows: Optional[int] = None,
	parse_dates: Optional[Sequence[str]] = None,
	dtype: Optional[Dict[str, object]] = None,
	usecols: Optional[Sequence[str]] = None,
	sep: str = ",",
	encoding: str = "utf-8",
) -> pd.DataFrame:
	"""Load a CSV from the project's raw data directory.

	Args:
		filename: Name of the CSV file inside `data/raw` (e.g. "flights.csv").
		nrows: If provided, read only this many rows (useful for sampling).
		parse_dates: List of column names to parse as dates.
		dtype: Dict mapping column name to dtype to enforce on read.
		usecols: Subset of columns to load.
		sep: CSV separator (default: ',').
		encoding: File encoding (default: 'utf-8').

	Returns:
		A pandas DataFrame with the loaded data.

	Raises:
		FileNotFoundError: If the file does not exist in data/raw.
		ValueError: If pandas fails to parse the file.
	"""
	path = RAW_DIR / filename
	if not path.exists():
		raise FileNotFoundError(f"Raw data file not found: {path}")

	try:
		df = pd.read_csv(
			path,
			nrows=nrows,
			parse_dates=list(parse_dates) if parse_dates is not None else None,
			dtype=dtype,
			usecols=list(usecols) if usecols is not None else None,
			sep=sep,
			encoding=encoding,
		)
	except Exception as exc:  # keep broad to surface parsing issues
		raise ValueError(f"Failed to read CSV {path}: {exc}") from exc

	return df
