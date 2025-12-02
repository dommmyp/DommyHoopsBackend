import duckdb
import re

# ---------------- CONFIG ----------------
DB_PATH = "/Users/dominicparolin/Code/dommyhoops/backend/cbb_data.duckdb"
TABLE   = "plays_team_flat_2025"          # change to your PBP table name
CHUNK_SIZE = 20_000      # tune chunk size if you want
# ----------------------------------------


slug_re = re.compile(r"[^a-z0-9]+")

def snake_case(s: str) -> str:
    s = (s or "").lower()
    s = slug_re.sub("_", s)
    return s.strip("_")


def parse_play_text(play_text: str, shooting_play: bool):
    """
    Parse playText into (play_type, player_name).

    Rules:
      - If shootingPlay is true: play_type = "shot"
        and player = name before " made " / " missed "
      - If text contains 'foul on <player>' or 'foul_on_player <player>':
        play_type = "foul", player = that <player>
      - For rebounds, avoid 'offensive'/'defensive' in the player name
      - Everything else falls back to a snake_case action + best-guess player
    """
    if not play_text:
        return "", ""

    text = play_text.strip()
    lower = text.lower()

    # 1) Shooting plays: force type "shot"
    if shooting_play:
        player = ""

        for kw in [" made ", " missed "]:
            idx = lower.find(kw)
            if idx != -1:
                player = text[:idx].strip(" .")
                break

        return "shot", player

    # 2) Fouls based on 'foul on <player>' in playText
    #    e.g. "Team foul on John Smith", "Foul on John Smith."
    m = re.search(r"foul on\s+(.+?)(?:[.:]|$)", text, flags=re.IGNORECASE)
    if m:
        player = m.group(1).strip(" .")
        return "foul", player

    # Also handle 'foul_on_player <player>' style if present
    m = re.search(r"foul_on_player[:\s]+(.+?)(?:[.:]|$)", text, flags=re.IGNORECASE)
    if m:
        player = m.group(1).strip(" .")
        return "foul", player

    # 3) Generic action parsing for non-shooting, non-explicit-foul plays
    # Order matters so rebounds work cleanly
    verbs = [
        " offensive rebound",
        " defensive rebound",
        " made ",
        " missed ",
        " turnover",
        " blocked ",
        " block ",
        " substitution",
        " rebound",
        " steal",
        " foul",    # generic fallback foul text if any
    ]

    best_idx = None
    best_verb = None

    for v in verbs:
        idx = lower.find(v)
        if idx != -1:
            if best_idx is None or idx < best_idx:
                best_idx = idx
                best_verb = v

    player = ""
    action = ""

    if best_idx is not None:
        # Everything before the verb is the player
        player = text[:best_idx].strip(" .")
        # Everything from the verb onwards is the action
        action = text[best_idx:].strip(" .")
    else:
        # Fallback: no recognizable verb, treat the whole thing as action
        action = text

    play_type = snake_case(action)
    return play_type, player


def main():
    con = duckdb.connect(DB_PATH)

    # 1) Ensure columns exist
    con.execute(f"""
        ALTER TABLE {TABLE}
        ADD COLUMN IF NOT EXISTS play_type VARCHAR
    """)
    con.execute(f"""
        ALTER TABLE {TABLE}
        ADD COLUMN IF NOT EXISTS player VARCHAR
    """)

    # 2) Count for progress
    total_rows = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    if total_rows == 0:
        print("No rows in table, nothing to do.")
        return

    print(f"Updating {total_rows:,} rows in {TABLE}...")

    # 3) Stream rows and update in chunks in a single transaction
    con.execute("BEGIN TRANSACTION")

    # Adjust column names if yours differ
    cur = con.cursor()
    cur.execute(
        f"SELECT rowid, playText, shootingPlay FROM {TABLE}"
    )

    processed = 0
    chunk_index = 0

    while True:
        rows = cur.fetchmany(CHUNK_SIZE)
        if not rows:
            break

        updates = []
        for rowid, play_text, shooting_play in rows:
            play_type, player = parse_play_text(
                play_text,
                bool(shooting_play),
            )
            updates.append((play_type, player, rowid))

        con.executemany(
            f"UPDATE {TABLE} SET play_type = ?, player = ? WHERE rowid = ?",
            updates,
        )

        processed += len(rows)
        chunk_index += 1
        pct = processed * 100.0 / total_rows
        print(f"Chunk {chunk_index}: {processed:,}/{total_rows:,} rows ({pct:.1f}%)")

    con.execute("COMMIT")
    con.close()

    print("Done.")


if __name__ == "__main__":
    main()

