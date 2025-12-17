#!/usr/bin/env bash
set -euo pipefail

: "${DISCORD_APP_ID:?missing DISCORD_APP_ID}"
: "${DISCORD_BOT_TOKEN:?missing DISCORD_BOT_TOKEN}"

api="https://discord.com/api/v10"
path="/applications/${DISCORD_APP_ID}/commands"
if [[ -n "${DISCORD_GUILD_ID:-}" ]]; then
  path="/applications/${DISCORD_APP_ID}/guilds/${DISCORD_GUILD_ID}/commands"
fi

payload='[
  {
    "name": "t",
    "type": 1,
    "description": "会話",
    "options": [
      {
        "name": "q",
        "type": 3,
        "description": "内容",
        "required": true
      }
    ]
  }
]'

curl -fsS -X PUT "${api}${path}" \
  -H "Authorization: Bot ${DISCORD_BOT_TOKEN}" \
  -H "Content-Type: application/json" \
  --data-binary "${payload}" >/dev/null

echo "ok"
