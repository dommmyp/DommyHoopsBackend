import os
import time
import json
import duckdb
import requests
import pandas as pd
from pandas import json_normalize

# -------------------------
# CONFIG
# -------------------------
API_URL = "https://api.collegebasketballdata.com/stats/player/season"

API_KEY = os.getenv("CBB_API_KEY", "+QsogsIaAexCzkcqU0tRf91CYMdgHD7cJgpWSG1dV97NVIYmGfIY7c8YyQbqqT8v")

HEADERS = {
    "accept": "application/json",
    "x-api-key": API_KEY if API_KEY else "",
    "Authorization": f"Bearer {API_KEY}",
}

SEASON       = 2025          # required
SEASON_TYPE  = "regular"     # "regular", "postseason", "preseason" or None
TEAM_FILTER  = None          # e.g. "Boise State"
CONF_FILTER  = None          # e.g. "MWC"
START_RANGE  = None          # e.g. "2024-11-01T00:00:00"
END_RANGE    = None          # e.g. "2025-03-31T23:59:59"

DUCKDB_PATH = "/Users/dominicparolin/Code/dommyhoops/backend/cbb_data.duckdb"
RAW_TABLE   = "player_stats_season_raw_2025"
FLAT_TABLE  = "player_stats_season_flat_2025"

MAX_RETRIES = 5
TIMEOUT_SEC = 60

# -------------------------
# HELPERS
# -------------------------
def _norm(name: str) -> str:
    return (
        str(name)
        .strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .lower()
    )

def backoff(attempt: int):
    time.sleep((attempt + 1) * 0.6)

def fetch_player_stats(season: int):
    """
    GET /stats/player/season with optional filters.
    """
    params = {"season": season}

    if SEASON_TYPE:
        params["seasonType"] = SEASON_TYPE
    if TEAM_FILTER:
        params["team"] = TEAM_FILTER
    if CONF_FILTER:
        params["conference"] = CONF_FILTER
    if START_RANGE:
        params["startDateRange"] = START_RANGE
    if END_RANGE:
        params["endDateRange"] = END_RANGE

    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(API_URL, headers=HEADERS, params=params, timeout=TIMEOUT_SEC)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                if attempt == MAX_RETRIES:
                    raise RuntimeError(f"HTTP {r.status_code} after retries: {r.text[:200]}")
                backoff(attempt)
                continue
            r.raise_for_status()
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            backoff(attempt)
    return []

# -------------------------
# TRANSFORMS
# -------------------------
def to_raw_df(records):
    """
    Direct DataFrame version. Lists and dicts stay as JSON strings.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # convert any list/dict columns to JSON strings
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
        )

    df.columns = [_norm(c) for c in df.columns]
    return df

def to_flat_df(records, sep="."):
    """
    Flatten nested dicts into top level columns.
    """
    if not records:
        return pd.DataFrame()

    flat = json_normalize(records, sep=sep, max_level=2)
    flat.columns = [_norm(c) for c in flat.columns]

    # optional unique key if these exist
    if all(c in flat.columns for c in ["playerid", "season"]):
        flat["unique_key"] = (
            flat["playerid"].astype("string") + "-" + flat["season"].astype("string")
        )

    return flat

# -------------------------
# DYNAMIC APPEND (same as roster script)
# -------------------------
def append_df(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame):
    if df.empty:
        return

    df = df.copy()
    df.columns = [_norm(c) for c in df.columns]

    exists = con.execute(
        f"SELECT COUNT(*)>0 FROM information_schema.tables WHERE table_name = '{_norm(table)}'"
    ).fetchone()[0]
    if not exists:
        con.register("tmp_df_init", df.head(0))
        con.execute(f"CREATE TABLE {_norm(table)} AS SELECT * FROM tmp_df_init")
        con.unregister("tmp_df_init")

    info = con.execute(f"PRAGMA table_info('{_norm(table)}')").fetchall()
    table_types = {_norm(row[1]): row[2].upper() for row in info}
    table_cols = list(table_types.keys())

    def duck_type_from_pd(dtype) -> str:
        if pd.api.types.is_integer_dtype(dtype):
            return "BIGINT"
        if pd.api.types.is_float_dtype(dtype):
            return "DOUBLE"
        if pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        return "VARCHAR"

    for col in [c for c in df.columns if c not in table_cols]:
        con.execute(
            f"ALTER TABLE {_norm(table)} ADD COLUMN {col} {duck_type_from_pd(df[col].dtype)}"
        )
        table_types[col] = duck_type_from_pd(df[col].dtype)
        table_cols.append(col)

    def needs_widen(current_duck: str, series: pd.Series):
        if current_duck in ("VARCHAR", "JSON"):
            return None
        if series.dtype == object:
            return "VARCHAR"
        if pd.api.types.is_float_dtype(series) and current_duck in (
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
        ):
            return "DOUBLE"
        if pd.api.types.is_datetime64_any_dtype(series) and current_duck != "TIMESTAMP":
            return "TIMESTAMP"
        return None

    for col in set(df.columns) & set(table_cols):
        tgt = needs_widen(table_types[col], df[col])
        if tgt:
            con.execute(
                f"ALTER TABLE {_norm(table)} ALTER COLUMN {col} TYPE {tgt}"
            )
            table_types[col] = tgt

    select_exprs = [(c if c in df.columns else f"NULL AS {c}") for c in table_cols]
    con.register("tmp_df", df)
    con.execute(
        f"INSERT INTO {_norm(table)} SELECT {', '.join(select_exprs)} FROM tmp_df"
    )
    con.unregister("tmp_df")

# -------------------------
# MAIN
# -------------------------
def main():
    if not API_KEY:
        raise RuntimeError("Set your API key in env var CBB_API_KEY or edit API_KEY")

    records = fetch_player_stats(SEASON)
    print(f"Fetched {len(records)} player-season rows for season {SEASON}")

    if not records:
        print("No /stats/player/season data returned")
        return

    raw_df = to_raw_df(records)
    flat_df = to_flat_df(records, sep=".")

    print("raw_df shape:", raw_df.shape)
    print("flat_df shape:", flat_df.shape)

    with duckdb.connect(DUCKDB_PATH) as con:
        append_df(con, RAW_TABLE, raw_df)
        append_df(con, FLAT_TABLE, flat_df)

    print(f"Done. Wrote player stats to {DUCKDB_PATH}")
    print(f"Tables: {RAW_TABLE}, {FLAT_TABLE}")

if __name__ == "__main__":
    main()

