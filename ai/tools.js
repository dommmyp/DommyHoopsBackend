// ai/tools.js

export const tools = [
  {
    type: "function",
    function: {
      name: "get_team_profile_2025",
      description:
        "Get 2025 team level info, including conference, adjusted ranking metrics, and basic record.",
      parameters: {
        type: "object",
        properties: {
          team: {
            type: "string",
            description: "Team name exactly as stored in the DB, for example 'Boise State'",
          },
        },
        required: ["team"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_team_game_log_2025",
      description:
        "Get all 2025 games for a team, with opponent, score, and date, filtered by optional opponent or date range.",
      parameters: {
        type: "object",
        properties: {
          team: { type: "string" },
          opponent: { type: "string", description: "Optional opponent team name" },
          min_date: { type: "string", description: "Inclusive, YYYY-MM-DD" },
          max_date: { type: "string", description: "Inclusive, YYYY-MM-DD" },
        },
        required: ["team"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_game_boxscore_2025",
      description:
        "Get team and player boxscore for a single 2025 game by gameId.",
      parameters: {
        type: "object",
        properties: {
          gameId: { type: "integer" },
        },
        required: ["gameId"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_player_season_stats_2025",
      description:
        "Get 2025 season long stats for a player, including per game numbers and efficiency stats.",
      parameters: {
        type: "object",
        properties: {
          playerName: {
            type: "string",
            description: "Player name as stored in the DB",
          },
          team: {
            type: "string",
            description: "Optional team name to disambiguate players with same name",
          },
        },
        required: ["playerName"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "get_player_game_log_2025",
      description:
        "Get 2025 game logs for a player, with points, minutes, shooting splits, and opponent.",
      parameters: {
        type: "object",
        properties: {
          playerName: { type: "string" },
          team: {
            type: "string",
            description: "Optional team name to disambiguate",
          },
        },
        required: ["playerName"],
      },
    },
  },
];

