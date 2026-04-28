#!/bin/bash
# digest-worker.sh — Process one jsonl session into a staging JSON file.
# Designed for `xargs -P N` parallel invocation. No shared writes to index.json.
#
# Usage: digest-worker.sh <jsonl-path>
#
# Outputs:
#   ${TEAM_DIR}/insights/staging/<sha-of-path>.json   array of {name,when_to_use,description,raw_hash} or []
#   ${TEAM_DIR}/insights/raw/<raw-hash>.json          original extracted content
#   ${TEAM_DIR}/.processed-files                       appends path on success (atomic via flock-free `>>`)
#
# Failure modes:
#   - extraction empty   → writes [] to staging, marks processed
#   - claudefast timeout → writes [] to staging, marks processed
#   - jq parse fail      → writes [] to staging, logs warn, marks processed
#   - any unhandled err  → exits non-zero, does NOT mark processed (will be retried next run)

set -u

JSONL="${1:-}"
[[ -z "${JSONL}" ]] && { echo "usage: $0 <jsonl-path>" >&2; exit 2; }
[[ ! -f "${JSONL}" ]] && { echo "missing: ${JSONL}" >&2; exit 2; }

TEAM_DIR="${HOME}/.claude-team"
STAGING_DIR="${TEAM_DIR}/insights/staging"
RAW_DIR="${TEAM_DIR}/insights/raw"
PROCESSED_LIST="${TEAM_DIR}/.processed-files"
WORKER_LOG="${TEAM_DIR}/logs/worker.log"

mkdir -p "${STAGING_DIR}" "${RAW_DIR}"

PATH_HASH=$(printf '%s' "${JSONL}" | shasum -a 256 | awk '{print $1}')
STAGING_PATH="${STAGING_DIR}/${PATH_HASH}.json"

# Already staged? Skip silently (idempotent).
if [[ -f "${STAGING_PATH}" ]]; then
  echo "[$(date +%H:%M:%S)] skip-staged $(basename "${JSONL}")" >> "${WORKER_LOG}"
  exit 0
fi

# Extract conversation content.
CONTENT=$(jq -r '
    select(.type=="user" or .type=="assistant")
    | . as $row
    | ($row.message.content // $row.content // null)
    | if . == null then empty
      elif type=="array" then
        map(select(.type=="text") | .text) | join("\n")
      else . end
    | select(. != null and . != "")
    | "[" + ($row.type) + "] " + .
  ' "${JSONL}" 2>/dev/null \
    | head -c 18000)

if [[ -z "${CONTENT}" ]]; then
  echo "[]" > "${STAGING_PATH}"
  echo "${JSONL}" >> "${PROCESSED_LIST}"
  echo "[$(date +%H:%M:%S)] empty $(basename "${JSONL}")" >> "${WORKER_LOG}"
  exit 0
fi

RAW_HASH=$(printf '%s' "${CONTENT}" | shasum -a 256 | awk '{print $1}')
RAW_PATH="${RAW_DIR}/${RAW_HASH}.json"
if [[ ! -f "${RAW_PATH}" ]]; then
  jq -n --arg c "${CONTENT}" --arg src "${JSONL}" \
        --arg t "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
     '{original_message:$c, source:$src, created_at:$t}' \
     > "${RAW_PATH}.tmp" && mv "${RAW_PATH}.tmp" "${RAW_PATH}"
fi

PROMPT="You are a Claude Code knowledge extractor. From this session log, extract 0-3 actionable insights or traps that another engineer would benefit from. Return ONLY a valid JSON array (no markdown, no prose, no explanation) where each item is {\"name\":string<=80, \"when_to_use\":string<=100, \"description\":string<=200}. If nothing notable, return []. Session log:
${CONTENT}"

RESPONSE=$(timeout 180 zsh -i -c "claudefast -p $(printf '%q' "${PROMPT}")" 2>>"${WORKER_LOG}" || echo "")

# Strip markdown fences and noise.
CLEAN=$(printf '%s' "${RESPONSE}" \
          | sed -e 's/^```json//' -e 's/^```//' -e 's/```$//' \
          | tr -d '\r' \
          | awk 'BEGIN{f=0} /^\[/{f=1} f{print} /\]$/{f=0}')

# Validate as JSON array.
if ! ARRAY=$(printf '%s' "${CLEAN}" | jq -c '. | if type=="array" then . else [] end' 2>/dev/null); then
  ARRAY="[]"
  echo "[$(date +%H:%M:%S)] unparseable $(basename "${JSONL}") resp_len=${#RESPONSE}" >> "${WORKER_LOG}"
fi

# Annotate each insight with raw_hash so the merger can dedupe + link.
RESULT=$(printf '%s' "${ARRAY}" | jq -c --arg rh "${RAW_HASH}" --arg src "${JSONL}" \
  'map({
     name: (.name // ""),
     when_to_use: (.when_to_use // ""),
     description: (.description // ""),
     raw_hash: $rh,
     source: $src
   } | select(.name != "" and .description != ""))')

[[ -z "${RESULT}" ]] && RESULT="[]"
printf '%s\n' "${RESULT}" > "${STAGING_PATH}"
echo "${JSONL}" >> "${PROCESSED_LIST}"

COUNT=$(printf '%s' "${RESULT}" | jq 'length' 2>/dev/null || echo 0)
echo "[$(date +%H:%M:%S)] ok $(basename "${JSONL}") n=${COUNT}" >> "${WORKER_LOG}"
