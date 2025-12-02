// server.js
// Run: npm i express cors compression duckdb
// Then: node server.js
import dotenv from "dotenv";
dotenv.config();
import express from "express"
import cors from "cors"
import compression from "compression"
import os from "node:os"
import path from "node:path"
import fs from "node:fs"
import { createRequire } from "node:module"
const require = createRequire(import.meta.url)
const duckdb = require("duckdb")


import agentRouter from "./ai/agentRouter.js";



// --- CONFIG ---
const SRC_DB = process.env.SRC_DB || "../cbb_data.duckdb";

const PORT = process.env.PORT || 4000

if (!fs.existsSync(SRC_DB)) {
  console.error(`[fatal] DB not found at ${SRC_DB}`)
  process.exit(1)
}

// Helper to convert BigInt to Number in objects
function convertBigInts(obj) {
  if (obj === null || obj === undefined) return obj
  if (typeof obj === 'bigint') return Number(obj)
  if (Array.isArray(obj)) return obj.map(convertBigInts)
  if (typeof obj === 'object') {
    return Object.fromEntries(
      Object.entries(obj).map(([k, v]) => [k, convertBigInts(v)])
    )
  }
  return obj
}

// --- APP SETUP ---
const app = express()
app.use(cors())
app.use(express.json({ limit: "10mb" }))
app.use(compression())
app.use("/api", agentRouter);

// --- DUCKDB SETUP ---
const db = new duckdb.Database(SRC_DB, duckdb.OPEN_READONLY)
const conn = db.connect()

const cpuCount = Math.max(1, os.cpus()?.length || 1)
const memForDuck = Math.max(256 * 1024 ** 2, Math.floor(os.totalmem() * 0.7))
const bytes = (n) => `${n}B`

// Configure DuckDB (synchronous, no await needed for these)
conn.run(`PRAGMA threads=${cpuCount}`)
conn.run(`PRAGMA memory_limit='${bytes(memForDuck)}'`)
conn.run(`PRAGMA preserve_insertion_order=false`)
conn.run(`PRAGMA enable_object_cache=true`)
conn.run(`PRAGMA temp_directory='${path.join(os.tmpdir(), "duckdb_tmp")}'`)

// --- CACHE HELPERS ---
const STMT_CACHE_LIMIT = 200
const RESULT_CACHE_LIMIT = 200
const CACHE_TTL_MS_DEFAULT = 30_000

const resultCache = new Map()

function cacheKey(sql, params) {
  return params ? `${sql}::${JSON.stringify(params)}` : sql
}

function cacheGet(k) {
  const v = resultCache.get(k)
  if (!v) return null
  if (v.exp < Date.now()) { resultCache.delete(k); return null }
  resultCache.delete(k)
  resultCache.set(k, v)
  return v.rows
}

function cacheSet(k, rows, ttl) {
  if (resultCache.size >= RESULT_CACHE_LIMIT) {
    const firstKey = resultCache.keys().next().value
    resultCache.delete(firstKey)
  }
  resultCache.set(k, { exp: Date.now() + ttl, rows })
}

async function qAll(sql, params = undefined, ttlMs = CACHE_TTL_MS_DEFAULT) {
  const key = cacheKey(sql, params)
  const hit = cacheGet(key)
  if (hit) return hit
  
  return new Promise((resolve, reject) => {
    const args = params ? (Array.isArray(params) ? params : [params]) : []
    conn.all(sql, ...args, (err, rows) => {
      if (err) {
        console.error('Query error:', err, 'SQL:', sql, 'Params:', params)
        reject(err)
      } else {
        const converted = convertBigInts(rows)
        cacheSet(key, converted, ttlMs)
        resolve(converted)
      }
    })
  })
}
const numberOrNull = (v) => (Number.isFinite(+v) ? +v : null)

// --- ROUTES ---
// Health check
app.get("/health", async (_req, res) => {
  try {
    const [{ v }] = await qAll("SELECT version() AS v", undefined, 5000)
    res.json({
      ok: true,
      duckdb: v,
      threads: cpuCount,
      memory_limit_bytes: memForDuck,
      db_path: SRC_DB
    })
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) })
  }
})

// Tables
app.get("/tables", async (_req, res) => {
  try {
    const rows = await qAll(
      `SELECT table_schema, table_name
       FROM information_schema.tables
       ORDER BY 1, 2`,
      undefined,
      60_000
    )
    res.set("Cache-Control", "public, max-age=60")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Rankings
app.get("/api/rankings", async (req, res) => {
  const year = String(req.query.year || "2025").trim()
  if (!/^\d{4}$/.test(year)) return res.status(400).json({ error: "Invalid year" })
  const page = Math.max(0, Number(req.query.page ?? 0))
  const size = 400
  const offset = page * size
  const tbl = `team_rankings_iter_${year}`
  
  try {
    const exists = await qAll(
      `SELECT 1 FROM information_schema.tables WHERE table_name = $1 LIMIT 1`,
      [tbl],
      60_000
    )
    
    if (exists.length === 0) return res.status(404).json({ error: `Table not found: ${tbl}` })
    
    const rows = await qAll(
      `SELECT * FROM ${tbl} ORDER BY rank ASC LIMIT $1 OFFSET $2`,
      [size, offset],
      30_000
    )
        console.log(rows)
    
    res.set("Cache-Control", "public, max-age=30")
    res.json(rows)
  } catch (e) {
    console.error('Rankings error:', e)
    res.status(500).json({ error: String(e) })
  }
})

// GET /api/stats/:team
app.get("/api/stats/team/:team", async (req, res) => {
  const year = String(req.query.year || "2025").trim()
  if (!/^\d{4}$/.test(year)) {
    return res.status(400).json({ error: "Invalid year" })
  }

  const teamName = decodeURIComponent(String(req.params.team || "")).trim()
  if (!teamName) {
    return res.status(400).json({ error: "Missing team name" })
  }

  const tbl = `team_rankings_iter_${year}`

  try {
    const exists = await qAll(
      `
      SELECT 1 
      FROM information_schema.tables 
      WHERE table_name = $1 
      LIMIT 1
      `,
      [tbl],
      60_000
    )

    if (exists.length === 0) {
      return res.status(404).json({ error: `Table not found: ${tbl}` })
    }

    const rows = await qAll(
      `
      SELECT *
      FROM ${tbl}
      WHERE LOWER(team) = LOWER($1)
      LIMIT 1
      `,
      [teamName],
      30_000
    )
    if (!rows || rows.length === 0) {
      return res
        .status(404)
        .json({ error: `Team '${teamName}' not found in ${tbl}` })
    }

    res.set("Cache-Control", "public, max-age=30")
    res.json(rows[0])
  } catch (e) {
    console.error("Team stats error:", e)
    res.status(500).json({ error: String(e) })
  }
})


// Teams
app.get("/api/teams", async (_req, res) => {
  try {
    const rows = await qAll(
      `SELECT DISTINCT teamid, team, conference
       FROM games_teams_flat_2025
       WHERE team IS NOT NULL
       ORDER BY team`,
      undefined,
      60_000
    )
    res.set("Cache-Control", "public, max-age=60")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Team roster
app.get("/api/team/:team/season-averages", async (req, res) => {
  const team = decodeURIComponent(req.params.team || "")
  try {
    const rows = await qAll(
      `
      WITH r AS (
        SELECT DISTINCT teamid, team, season
        FROM teams_roster_flat_2025
        WHERE team = $1
      )
      SELECT r.teamid, r.team, r.season,
             p.id AS athleteid, p.name, p.position, p.jersey,
             p.hometown_city AS city, p.hometown_state AS state,
             p.height, p.weight, p.startseason, p.endseason
      FROM teams_roster_flat_2025 p
      JOIN r ON r.teamid = p.teamid AND r.season = p.season
      ORDER BY name
      `,
      [team],
      120_000
    )
    res.set("Cache-Control", "public, max-age=120")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Team player stats for a season
app.get("/api/team/:team/player-stats", async (req, res) => {
  const team = decodeURIComponent(req.params.team || "").trim()
  const season = numberOrNull(req.query.season ?? 2025)

  if (!team) {
    return res.status(400).json({ error: "Missing team name" })
  }
  if (season === null) {
    return res.status(400).json({ error: "Invalid season" })
  }

  try {
    const rows = await qAll(
      `
      SELECT *
      FROM player_stats_season_flat_2025
      WHERE season = $1
        AND LOWER(team) = LOWER($2)
      `,
      [season, team],
      60_000
    )

    res.set("Cache-Control", "public, max-age=60")
    res.json(rows)
  } catch (e) {
    console.error("Team player-stats error:", e)
    res.status(500).json({ error: String(e) })
  }
})


// Team schedule
app.get("/api/team/:team/schedule", async (req, res) => {
  const team = decodeURIComponent(req.params.team || "")
  try {
    const rows = await qAll(
      `
      SELECT id AS gameid, season, seasonlabel, seasontype, status,
             strftime(startdate, '%Y-%m-%d %H:%M:%S') AS startdate,
             hometeam, hometeamid, homeconference, homepoints,
             awayteam, awayteamid, awayconference, awaypoints,
             venue, city, state, tournament, gametype, gamenotes
      FROM games_flat_2025
      WHERE hometeam = $1 OR awayteam = $2
      ORDER BY startdate
      `,
      [team, team],
      120_000
    )

    res.set("Cache-Control", "public, max-age=120")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Game details
app.get("/api/game/:gameId", async (req, res) => {
  const gameId = numberOrNull(req.params.gameId)
  if (gameId === null) return res.status(400).json({ error: "Invalid gameId" })
  try {
    const rows = await qAll(
      `
      SELECT id AS gameid, season, seasonlabel, seasontype, status,
             startdate,
             hometeam, awayteam, homepoints, awaypoints,
             homeconference, awayconference, venue, city, state,
             excitement, attendance, tournament, gametype, gamenotes
      FROM games_flat_2025
      WHERE id = $1
      LIMIT 1
      `,
      [gameId],
      30_000
    )
    res.set("Cache-Control", "public, max-age=30")
    res.json(rows[0] || null)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Plays
app.get("/api/game/:gameId/plays", async (req, res) => {
  const gameId = numberOrNull(req.params.gameId)
  if (gameId === null) return res.status(400).json({ error: "Invalid gameId" })
  try {
    const rows = await qAll(
      `
      SELECT *
      FROM plays_flat_2025
      WHERE gameid = $1
      ORDER BY
        COALESCE(period, 0) ASC,
        COALESCE(secondsremaining, 0) DESC,
        COALESCE(id, 0) ASC
      `,
      [gameId],
      30_000
    )
    res.set("Cache-Control", "public, max-age=30")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Team plays
app.get("/api/team/:team/plays", async (req, res) => {
  const team = decodeURIComponent(req.params.team || "").trim()
  const season = numberOrNull(req.query.season ?? 2025)
  if (!team) return res.status(400).json({ error: "Missing team name" })
  if (season === null) return res.status(400).json({ error: "Invalid season" })
  try {
    const rows = await qAll(
      `
      SELECT *
      FROM plays_flat_2025
      WHERE season = $1 AND LOWER(team) = LOWER($2)
      ORDER BY
        COALESCE(period, 0) ASC,
        COALESCE(secondsremaining, 0) DESC,
        COALESCE(id, 0) ASC
      `,
      [season, team],
      30_000
    )
    res.set("Cache-Control", "public, max-age=30")
    res.json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Boxscore
app.get("/api/game/:gameId/boxscore", async (req, res) => {
  const gameId = numberOrNull(req.params.gameId)
  if (gameId === null) return res.status(400).json({ error: "Invalid gameId" })
  try {
    const [teams, players] = await Promise.all([
      qAll(
        `
        SELECT teamid, team, opponentid, opponent,
               teamstats_points_total AS points,
               opponentstats_points_total AS opp_points,
               teamstats_possessions  AS poss,
               teamstats_fieldgoals_made   AS fg_made,
               teamstats_fieldgoals_attempted AS fg_att,
               teamstats_threepointfieldgoals_made AS tp_made,
               teamstats_threepointfieldgoals_attempted AS tp_att,
               teamstats_freethrows_made AS ft_made,
               teamstats_freethrows_attempted AS ft_att,
               teamstats_rebounds_offensive AS oreb,
               teamstats_rebounds_defensive AS dreb,
               teamstats_turnovers_total AS tov,
               teamstats_assists AS ast, teamstats_blocks AS blk, teamstats_steals AS stl
        FROM games_teams_flat_2025
        WHERE gameid = $1
        `,
        [gameId],
        60_000
      ),
      qAll(
        `
        SELECT gameid, team, teamid, name, athleteid, minutes, points,
               fieldgoals_made, fieldgoals_attempted,
               threepointfieldgoals_made, threepointfieldgoals_attempted,
               freethrows_made, freethrows_attempted,
               rebounds_offensive, rebounds_defensive, assists, turnovers, blocks, steals
        FROM games_players_flat_2025    
        WHERE gameid = $1
        `,
        [gameId],
        60_000
      )
    ])
    res.set("Cache-Control", "public, max-age=60")
    res.json({ teams, players })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})

// Team info
app.get("/api/team/:teamName/info", async (req, res) => {
  const teamName = decodeURIComponent(req.params.teamName || "").trim()
  if (!teamName) return res.status(400).json({ error: "Missing team name" })
  try {
    const rows = await qAll(
      `SELECT * FROM teams_raw WHERE LOWER(school) = LOWER($1) LIMIT 1`,
      [teamName],
      300_000
    )
    if (!rows || rows.length === 0)
      return res.status(404).json({ error: `Team '${teamName}' not found` })
    res.set("Cache-Control", "public, max-age=300")
    res.json(rows[0])
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
})


// Player season info (aggregates / averages)
app.get("/api/player/:playerId", async (req, res) => {
  const playerIdRaw = String(req.params.playerId || "").trim()
  const season = numberOrNull(req.query.year ?? 2025)


  if (!playerIdRaw) {
    return res.status(400).json({ error: "Missing playerId" })
  }
  if (season === null) {
    return res.status(400).json({ error: "Invalid year" })
  }

  const playerIdNum = numberOrNull(playerIdRaw)

  try {
    const rows = await qAll(
      `
      SELECT *
      FROM player_stats_season_flat_2025
      WHERE season = $1
        AND athletesourceid = $2
      LIMIT 1
      `,
      [season, playerIdNum ?? playerIdRaw],
      60_000
    )

    const row = rows && rows.length > 0 ? rows[0] : null

    res.set("Cache-Control", "public, max-age=60")
    res.json(row)
  } catch (e) {
    console.error("Player info error:", e)
    res.status(500).json({ error: String(e) })
  }
})

// Player game log for a season
app.get("/api/player/:playerId/gamelog", async (req, res) => {
  const playerIdRaw = String(req.params.playerId || "").trim()
  const season = numberOrNull(req.query.year ?? 2025)


  if (!playerIdRaw) {
    return res.status(400).json({ error: "Missing playerId" })
  }
  if (season === null) {
    return res.status(400).json({ error: "Invalid year" })
  }

  const playerIdNum = numberOrNull(playerIdRaw)

  try {
    const rows = await qAll(
      `
      SELECT
        gp.gameid,
        g.season,
        g.seasonlabel,
        g.seasontype,
        g.status,
        strftime(g.startdate, '%Y-%m-%d %H:%M:%S') AS startdate,
        g.hometeam,
        g.awayteam,
        g.homepoints,
        g.awaypoints,
        g.venue,
        g.city,
        g.state,
        gp.team,
        gp.teamid,
        gp.name,
        gp.athletesourceid,
        gp.minutes,
        gp.points,
        gp.rebounds_offensive,
        gp.rebounds_defensive,
        (gp.rebounds_offensive + gp.rebounds_defensive) AS rebounds_total,
        gp.assists,
        gp.fieldgoals_made       AS fgm,
        gp.fieldgoals_attempted  AS fga,
        gp.threepointfieldgoals_made      AS fg3m,
        gp.threepointfieldgoals_attempted AS fg3a,
        gp.freethrows_made       AS ftm,
        gp.freethrows_attempted  AS fta,
        gp.turnovers,
        gp.blocks,
        gp.steals
      FROM games_players_flat_2025 gp
      JOIN games_flat_2025 g
        ON g.id = gp.gameid
      WHERE g.season = $1
        AND gp.athletesourceid = $2
      ORDER BY g.startdate
      `,
      [season, playerIdNum ?? playerIdRaw],
      60_000
    )

    res.set("Cache-Control", "public, max-age=60")
    res.json(Array.isArray(rows) ? rows : [])
  } catch (e) {
    console.error("Player gamelog error:", e)
    res.status(500).json({ error: String(e) })
  }
})

app.get("/api/game/:gameId/pbp", (req, res) => {
  const gameId = Number(req.params.gameId);
  if (!Number.isFinite(gameId)) {
    res.status(400).json({ error: "invalid gameId" });
    return;
  }

  const sql = `
    SELECT
      gameId,
      season,
      seasonType,
      gameType,
      tournament,
      playType,
      isHomeTeam,
      teamId,
      team,
      conference,
      opponentId,
      opponent,
      opponentConference,
      homeScore,
      awayScore,
      period,
      clock,
      secondsRemaining,
      scoringPlay,
      shootingPlay,
      scoreValue,
      wallclock,
      playText,
      play_type,
      player,
      "shotInfo.shooter.id"   AS shooterId,
      "shotInfo.shooter.name" AS shooterName,
      "shotInfo.made"         AS shotMade,
      "shotInfo.range"        AS shotRange
    FROM plays_team_flat_2025
    WHERE gameId = ?
    ORDER BY period ASC, secondsRemaining DESC, id ASC
  `;

  db.all(sql, [gameId], (err, rows) => {
    if (err) {
      console.error("pbp query error", err);
      res.status(500).json({ error: "pbp query failed" });
      return;
    }
    res.json(convertBigInts(rows));
  });
});



app.get("/api/game/:gameId/shots", (req, res) => {
  const gameId = Number(req.params.gameId);
  if (!Number.isFinite(gameId)) {
    res.status(400).json({ error: "invalid gameId" });
    return;
  }

  const sql = `
    SELECT
      gameId,
      period,
      clock,
      secondsRemaining,
      isHomeTeam,
      teamId,
      team,
      opponent,
      homeScore,
      awayScore,
      scoringPlay,
      shootingPlay,
      scoreValue,
      playText,
      "shotInfo.shooter.id"   AS shooterId,
      "shotInfo.shooter.name" AS shooterName,
      "shotInfo.made"         AS shotMade,
      "shotInfo.range"        AS shotRange,
      "shotInfo.location.x"   AS x,
      "shotInfo.location.y"   AS y
    FROM plays_team_flat_2025
    WHERE gameId = ?
      AND shootingPlay
      AND "shotInfo.location.x" IS NOT NULL
      AND "shotInfo.location.y" IS NOT NULL
    ORDER BY period ASC, secondsRemaining DESC, id ASC
  `;

  db.all(sql, [gameId], (err, rows) => {
    if (err) {
      console.error("shots query error", err);
      res.status(500).json({ error: "shots query failed" });
      return;
    }
    res.json(convertBigInts(rows));
  });
});




// --- START SERVER ---
app.listen(PORT, () => {
  console.log(`API listening on http://localhost:${PORT}`)
  console.log(`[info] DB: ${SRC_DB}`)
})
