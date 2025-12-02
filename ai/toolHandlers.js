import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const duckdb = require("duckdb");

// ------------------------
// DuckDB setup
// ------------------------
const SRC_DB =
  process.env.SRC_DB ||
  "/Users/dominicparolin/Code/dommyhoops/backend/cbb_data.duckdb";

const db = new duckdb.Database(SRC_DB, duckdb.OPEN_READONLY);
const conn = db.connect();

// Optimization Pragmas
const cpuCount = Math.max(1, os.cpus()?.length || 1);
const memForDuck = Math.max(256 * 1024 ** 2, Math.floor(os.totalmem() * 0.3));
const bytes = (n) => `${n}B`;

conn.run(`PRAGMA threads=${cpuCount}`);
conn.run(`PRAGMA memory_limit='${bytes(memForDuck)}'`);
conn.run(`PRAGMA preserve_insertion_order=false`);
conn.run(`PRAGMA temp_directory='${path.join(os.tmpdir(), "duckdb_ai_tmp")}'`);

// ------------------------
// Helpers
// ------------------------
function convertBigInts(obj) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === "bigint") return Number(obj);
  if (Array.isArray(obj)) return obj.map(convertBigInts);
  if (typeof obj === "object") {
    return Object.fromEntries(
      Object.entries(obj).map(([k, v]) => [k, convertBigInts(v)])
    );
  }
  return obj;
}

function qAll(sql, params = []) {
  const args = Array.isArray(params) ? params : [params];
  return new Promise((resolve, reject) => {
    conn.all(sql, ...args, (err, rows) => {
      if (err) {
        console.error("[AI qAll] error", err, "SQL:", sql);
        return reject(err);
      }
      resolve(convertBigInts(rows));
    });
  });
}

// ------------------------
// 1. TEAM HANDLERS
// ------------------------

async function get_team_overview({ team }) {
  const sql = `
    SELECT * FROM team_rankings_adj_2025
    WHERE LOWER(team) = LOWER($1) LIMIT 1
  `;
  const rows = await qAll(sql, [team]);
  return rows.length ? rows[0] : { error: "Team not found" };
}

async function get_team_roster({ team }) {
  // Assuming roster info is in player stats or a dedicated roster table
  // Using player_stats_season as a proxy for roster
  const sql = `
    SELECT *
    FROM player_stats_season_flat_2025
    WHERE LOWER(team) = LOWER($1)
    ORDER BY minutes DESC
  `;
  const rows = await qAll(sql, [team]);
  return { team, roster_count: rows.length, roster: rows };
}

async function get_team_schedule({ team, limit = 20 }) {
  const sql = `
    SELECT * FROM games_flat_2025
    WHERE (LOWER(hometeam) = LOWER($1) OR LOWER(awayteam) = LOWER($1))
    ORDER BY startdate DESC
    LIMIT $2
  `;
  const rows = await qAll(sql, [team, limit]);
  return { team, type: "past_games", games: rows };
}

async function get_team_splits({ team }) {
  // Aggregating home vs away performance
  // Note: This requires complex logic to determine if 'team' was home or away in the row
  const sql = `
    WITH team_games AS (
      SELECT 
        CASE 
          WHEN LOWER(hometeam) = LOWER($1) THEN 'Home' 
          WHEN venue_neutral = true THEN 'Neutral'
          ELSE 'Away' 
        END as location,
        CASE 
          WHEN LOWER(hometeam) = LOWER($1) THEN homepoints 
          ELSE awaypoints 
        END as pts_scored,
        CASE 
          WHEN LOWER(hometeam) = LOWER($1) THEN awaypoints 
          ELSE homepoints 
        END as pts_allowed
      FROM games_flat_2025
      WHERE (LOWER(hometeam) = LOWER($1) OR LOWER(awayteam) = LOWER($1))
        AND status = 'Final'
    )
    SELECT 
      location, 
      COUNT(*) as games,
      AVG(pts_scored)::INT as avg_scored,
      AVG(pts_allowed)::INT as avg_allowed,
      AVG(pts_scored - pts_allowed)::DECIMAL(5,1) as avg_margin
    FROM team_games
    GROUP BY location
  `;
  const rows = await qAll(sql, [team]);
  return { team, splits: rows };
}

async function get_team_record_vs_ranked({ team }) {
  // Determine if opponent was ranked (using a simplified Top 25 check from rankings table)
  const sql = `
    SELECT 
      g.startdate, g.hometeam, g.awayteam, g.homepoints, g.awaypoints,
      r.rank as opponent_rank
    FROM games_flat_2025 g
    JOIN team_rankings_adj_2025 r 
      ON (LOWER(r.team) = LOWER(g.hometeam) OR LOWER(r.team) = LOWER(g.awayteam))
    WHERE (LOWER(g.hometeam) = LOWER($1) OR LOWER(g.awayteam) = LOWER($1))
      AND LOWER(r.team) != LOWER($1)
      AND r.rank <= 25
      AND g.status = 'Final'
    ORDER BY g.startdate DESC
  `;
  const rows = await qAll(sql, [team]);
  return { team, games_vs_top_25: rows };
}

/**
 * NEW HANDLER: get_team_player_stats_by_season
 * Gets seasonal stats for all players on a team.
 */
async function get_team_player_stats_by_season({ team }) {
  const sql = `
    SELECT *
    FROM player_stats_season_flat_2025
    WHERE LOWER(team) = LOWER($1)
    ORDER BY minutes DESC
  `;
  const rows = await qAll(sql, [team]);
  return { team, player_stats: rows };
}

// ------------------------
// 2. PLAYER HANDLERS
// ------------------------

async function get_player_season_stats({ player_name, team }) {
  let sql = `SELECT * FROM player_stats_season_flat_2025 WHERE LOWER(name) = LOWER($1)`;
  const params = [player_name];
  if (team) {
    sql += ` AND LOWER(team) = LOWER($2)`;
    params.push(team);
  }
  const rows = await qAll(sql, params);
  return rows.length ? rows[0] : { error: "Player not found" };
}

async function get_player_game_log({ player_name, limit = 10 }) {
  // Assumes a table 'player_game_stats_2025' exists
  const sql = `
    SELECT *
    FROM games_players_flat_2025
    WHERE LOWER(name) = LOWER($1)
    ORDER BY startdate DESC
    LIMIT $2
  `;
  try {
    const rows = await qAll(sql, [player_name, limit]);
    return { player_name, logs: rows };
  } catch (e) {
    return { error: "Player game logs table not found or query failed." };
  }
}

async function search_players({ query, team }) {
  let sql = `
    SELECT *
    FROM player_stats_season_flat_2025 
    WHERE name ILIKE '%' || $1 || '%'
  `;
  const params = [query];
  if (team) {
    sql += ` AND LOWER(team) = LOWER($2)`;
    params.push(team);
  }
  sql += ` LIMIT 10`;
  const rows = await qAll(sql, params);
  return { query, results: rows };
}

async function compare_two_players({ player1, player2 }) {
  const sql = `
    SELECT * FROM player_stats_season_flat_2025 
    WHERE LOWER(name) IN (LOWER($1), LOWER($2))
  `;
  const rows = await qAll(sql, [player1, player2]);
  return { comparison: rows };
}

// ------------------------
// 3. GAME & MATCHUP HANDLERS
// ------------------------

async function get_game_boxscore({ game_id }) {
  // Assumes player_game_stats has a game_id column
  const sql = `
    SELECT team, player_name, points, rebounds, assists, minutes 
    FROM player_game_stats_2025
    WHERE game_id = $1
    ORDER BY team, minutes DESC
  `;
  try {
    const rows = await qAll(sql, [game_id]);
    return { game_id, boxscore: rows };
  } catch (e) {
    return { error: "Boxscore data not available." };
  }
}

async function get_games_by_date({ date, conference }) {
  let sql = `SELECT * FROM games_flat_2025 WHERE startdate = $1`;
  const params = [date];
  // Note: We'd need to join rankings/teams to filter by conference effectively
  // This is a simplified version
  const rows = await qAll(sql, params);
  return { date, games: rows };
}

async function get_head_to_head({ team1, team2 }) {
  const sql = `
    SELECT * FROM games_flat_2025
    WHERE (
      (LOWER(hometeam) = LOWER($1) AND LOWER(awayteam) = LOWER($2)) OR 
      (LOWER(hometeam) = LOWER($2) AND LOWER(awayteam) = LOWER($1))
    )
    AND status = 'Final'
    ORDER BY startdate DESC
  `;
  const rows = await qAll(sql, [team1, team2]);
  return { matchup: `${team1} vs ${team2}`, history: rows };
}

async function get_daily_top_performers({ date, stat = 'points' }) {
  // Map friendly stat name to DB column
  const colMap = { points: 'points', rebounds: 'rebounds', assists: 'assists' };
  const col = colMap[stat] || 'points';
  
  const sql = `
    SELECT player_name, team, points, rebounds, assists 
    FROM player_game_stats_2025
    WHERE date = $1
    ORDER BY ${col} DESC
    LIMIT 5
  `;
  try {
    const rows = await qAll(sql, [date]);
    return { date, stat, leaders: rows };
  } catch (e) {
    return { error: "Daily stats not available." };
  }
}

// ------------------------
// 4. RANKINGS & LEADERS HANDLERS
// ------------------------

async function get_team_rankings({ limit = 25 }) {
  const sql = `SELECT * FROM team_rankings_adj_2025 ORDER BY rank ASC LIMIT $1`;
  const rows = await qAll(sql, [limit]);
  return { rankings: rows };
}

async function get_conference_standings({ conference }) {
  const sql = `
    SELECT rank, team, conf, record_overall, adj_o, adj_d 
    FROM team_rankings_adj_2025
    WHERE LOWER(conf) = LOWER($1)
    ORDER BY rank ASC
  `;
  const rows = await qAll(sql, [conference]);
  return { conference, standings: rows };
}

async function get_player_stat_leaders({ stat_category, limit = 10 }) {
  // Map category to column
  const map = {
    points: 'pts_pg', rebounds: 'reb_pg', assists: 'ast_pg', 
    blocks: 'blk_pg', steals: 'stl_pg', per: 'per', usage: 'usage_rate'
  };
  const col = map[stat_category] || 'pts_pg';

  const sql = `
    SELECT name, team, ${col} as value 
    FROM player_stats_season_flat_2025
    ORDER BY ${col} DESC
    LIMIT $1
  `;
  const rows = await qAll(sql, [limit]);
  return { stat: stat_category, leaders: rows };
}

async function get_team_stat_leaders({ stat_category, limit = 10 }) {
  const map = {
    ppg: 'adj_o', // Using Efficiency as proxy if PPG not in rankings
    opp_ppg: 'adj_d',
    adjo: 'adj_o',
    adjd: 'adj_d',
    tempo: 'adj_tempo',
    '3pt_pct': 'three_pt_pct' // Assuming this column exists
  };
  const col = map[stat_category] || 'adj_o';
  
  const sql = `
    SELECT team, ${col} as value 
    FROM team_rankings_adj_2025
    ORDER BY ${col} ${stat_category === 'adjd' || stat_category === 'opp_ppg' ? 'ASC' : 'DESC'}
    LIMIT $1
  `;
  const rows = await qAll(sql, [limit]);
  return { stat: stat_category, leaders: rows };
}

async function get_biggest_upsets({ limit = 5 }) {
  // This looks for games where the winner had a much worse rank (higher number) than the loser
  const sql = `
    SELECT 
      g.startdate, g.hometeam, g.awayteam, g.homepoints, g.awaypoints,
      r_home.rank as home_rank, r_away.rank as away_rank,
      CASE 
        WHEN g.homepoints > g.awaypoints THEN r_home.rank - r_away.rank
        ELSE r_away.rank - r_home.rank
      END as upset_magnitude
    FROM games_flat_2025 g
    JOIN team_rankings_adj_2025 r_home ON LOWER(g.hometeam) = LOWER(r_home.team)
    JOIN team_rankings_adj_2025 r_away ON LOWER(g.awayteam) = LOWER(r_away.team)
    WHERE g.status = 'Final'
    ORDER BY upset_magnitude DESC
    LIMIT $1
  `;
  const rows = await qAll(sql, [limit]);
  return { upsets: rows };
}

async function get_bubble_teams() {
  const sql = `
    SELECT rank, team, conf, record_overall 
    FROM team_rankings_adj_2025 
    WHERE rank BETWEEN 40 AND 60 
    ORDER BY rank ASC
  `;
  const rows = await qAll(sql);
  return { bubble_teams: rows };
}

async function get_best_home_court_advantage() {
  // Finds teams with biggest differential between Home Margin and Away Margin
  // This is a "heavy" query, doing it best effort
  const sql = `
    WITH home_stats AS (
      SELECT hometeam as team, AVG(homepoints - awaypoints) as margin
      FROM games_flat_2025 WHERE status='Final' GROUP BY hometeam
    ),
    away_stats AS (
      SELECT awayteam as team, AVG(awaypoints - homepoints) as margin
      FROM games_flat_2025 WHERE status='Final' GROUP BY awayteam
    )
    SELECT 
      h.team, 
      CAST(h.margin AS DECIMAL(5,1)) as home_margin, 
      CAST(a.margin AS DECIMAL(5,1)) as away_margin,
      CAST((h.margin - a.margin) AS DECIMAL(5,1)) as home_court_diff
    FROM home_stats h
    JOIN away_stats a ON h.team = a.team
    ORDER BY home_court_diff DESC
    LIMIT 10
  `;
  const rows = await qAll(sql);
  return { best_home_advantages: rows };
}

async function get_best_performances_season({ limit = 5 }) {
  // Assumes player_game_stats table
  const sql = `
    SELECT player_name, team, date, points, opponent 
    FROM player_game_stats_2025 
    ORDER BY points DESC 
    LIMIT $1
  `;
  try {
    const rows = await qAll(sql, [limit]);
    return { top_scorers: rows };
  } catch (e) {
    return { error: "Player game stats table missing." };
  }
}

// ------------------------
// EXPORTS
// ------------------------

export const toolHandlers = {
  get_team_overview,
  get_team_roster,
  get_team_schedule,
  get_team_splits,
  get_team_record_vs_ranked,
  get_team_player_stats_by_season, // <- NEW HANDLER
  get_player_season_stats,
  get_player_game_log,
  search_players,
  compare_two_players,
  get_game_boxscore,
  get_games_by_date,
  get_head_to_head,
  get_daily_top_performers,
  get_team_rankings,
  get_conference_standings,
  get_player_stat_leaders,
  get_team_stat_leaders,
  get_biggest_upsets,
  get_bubble_teams,
  get_best_home_court_advantage,
  get_best_performances_season
};

export const toolDefinitions = [
  // ==========================================================
  // 1. TEAM LEVEL TOOLS
  // ==========================================================
  {
    type: "function",
    function: {
      name: "get_team_overview",
      description: "Get high-level 2025 profile: record, conference, adjusted efficiency metrics (AdjO, AdjD), and overall rank.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name, e.g. 'Gonzaga'." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_team_roster",
      description: "Get the full roster for a specific team, including jersey numbers, positions, heights, and year.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_team_player_stats_by_season",
      description: "Get season stats (PPG, RPG, APG, PER) for all players on a specific team. Useful for 'Show me the Duke roster stats'.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  }, // <- NEW DEFINITION
  {
    type: "function",
    function: {
      name: "get_team_schedule",
      description: "Get game log for a team in 2025, including scores and opponents.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name." },
          limit: { type: "integer", description: "Max games to return (default 40)." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_team_splits",
      description: "Get team performance splits: Home vs Away vs Neutral. Useful for 'Are they better at home?'.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_team_record_vs_ranked",
      description: "Get a team's win/loss record specifically against Top 25 opponents. Useful for resume analysis.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string", description: "Exact team name." }
        },
        required: ["team"],
        additionalProperties: false
      }
    }
  },

  // ==========================================================
  // 2. PLAYER LEVEL TOOLS
  // ==========================================================
  {
    type: "function",
    function: {
      name: "get_player_season_stats",
      description: "Get season aggregates (PPG, RPG, APG, Shooting %) and efficiency stats (PER, Usage) for a player.",
      parameters: {
        type: "object",
        properties: {
          player_name: { type: "string", description: "Player's full name." },
          team: { type: "string", description: "Optional team name to help filter." }
        },
        required: ["player_name"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_player_game_log",
      description: "Get a player's stats for specific recent games (points, assists, rebounds in each game).",
      parameters: {
        type: "object",
        properties: {
          player_name: { type: "string", description: "Player's full name." },
          limit: { type: "integer", description: "Number of recent games to return (default 10)." }
        },
        required: ["player_name"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "search_players",
      description: "Fuzzy search for a player name if the exact spelling is unknown or to find all players with a last name.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Partial name, e.g. 'Bacot' or 'Caleb'." },
          team: { type: "string", description: "Optional team filter." }
        },
        required: ["query"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "compare_two_players",
      description: "Get a side-by-side statistical comparison of two players for the 2025 season.",
      parameters: {
        type: "object",
        properties: {
          player1: { type: "string", description: "Name of first player." },
          player2: { type: "string", description: "Name of second player." }
        },
        required: ["player1", "player2"],
        additionalProperties: false
      }
    }
  },

  // ==========================================================
  // 3. GAME & MATCHUP TOOLS
  // ==========================================================
  {
    type: "function",
    function: {
      name: "get_game_boxscore",
      description: "Get the full box score (player stats for both teams) for a specific game ID.",
      parameters: {
        type: "object",
        properties: {
          game_id: { type: "integer", description: "The unique ID of the game found via schedule tools." }
        },
        required: ["game_id"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_games_by_date",
      description: "Get all games played on a specific date. Useful for 'What happened last night?'.",
      parameters: {
        type: "object",
        properties: {
          date: { type: "string", description: "Date in YYYY-MM-DD format." },
          conference: { type: "string", description: "Optional conference filter (e.g., 'ACC')." }
        },
        required: ["date"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_head_to_head",
      description: "Get historical matchup history between two specific teams (recent meetings).",
      parameters: {
        type: "object",
        properties: {
          team1: { type: "string" },
          team2: { type: "string" }
        },
        required: ["team1", "team2"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_daily_top_performers",
      description: "Get the players with the most points/rebounds/assists for a specific date.",
      parameters: {
        type: "object",
        properties: {
          date: { type: "string", description: "Date in YYYY-MM-DD format." },
          stat: { type: "string", enum: ["points", "rebounds", "assists"], description: "Stat to sort by." }
        },
        required: ["date"],
        additionalProperties: false
      }
    }
  },

  // ==========================================================
  // 4. RANKINGS, LEADERS & CONFERENCE TOOLS
  // ==========================================================
  {
    type: "function",
    function: {
      name: "get_team_rankings",
      description: "Get the top N teams based on DommyHoops predictive rankings.",
      parameters: {
        type: "object",
        properties: {
          limit: { type: "integer", description: "Number of teams (default 25)." }
        },
        required: [],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_conference_standings",
      description: "Get the current W-L standings for a specific conference.",
      parameters: {
        type: "object",
        properties: {
          conference: { type: "string", description: "Conference abbreviation, e.g., 'Big 12', 'SEC', 'Big East'." }
        },
        required: ["conference"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_player_stat_leaders",
      description: "Get the top players in the nation for a specific stat (PPG, RPG, APG, FG%, etc.).",
      parameters: {
        type: "object",
        properties: {
          stat_category: { 
            type: "string", 
            enum: ["points", "rebounds", "assists", "blocks", "steals", "per", "usage"],
            description: "The statistical category to sort by." 
          },
          limit: { type: "integer", description: "Top N players (default 10)." }
        },
        required: ["stat_category"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_team_stat_leaders",
      description: "Get the top teams in the nation for a specific stat (Points Per Game, AdjO, AdjD, Pace).",
      parameters: {
        type: "object",
        properties: {
          stat_category: { 
            type: "string", 
            enum: ["ppg", "opp_ppg", "adjo", "adjd", "tempo", "3pt_pct"],
            description: "The statistical category to sort by." 
          },
          limit: { type: "integer", description: "Top N teams (default 10)." }
        },
        required: ["stat_category"],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_biggest_upsets",
      description: "Get a list of games where a lower-ranked team defeated a highly-ranked team recently.",
      parameters: {
        type: "object",
        properties: {
          limit: { type: "integer", default: 5 }
        },
        required: [],
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_bubble_teams",
      description: "Get a list of teams that are on the 'bubble' (ranked roughly 40-60).",
      parameters: {
        type: "object",
        properties: {},
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_best_home_court_advantage",
      description: "Get teams with the largest discrepancy between home and away performance.",
      parameters: {
        type: "object",
        properties: {},
        additionalProperties: false
      }
    }
  },
  {
    type: "function",
    function: {
      name: "get_best_performances_season",
      description: "Get the highest single-game scoring performances by any player this season.",
      parameters: {
        type: "object",
        properties: {
            limit: { type: "integer", default: 5 }
        },
        additionalProperties: false
      }
    }
  }
];

export default {
  toolHandlers,
  toolDefinitions
};
