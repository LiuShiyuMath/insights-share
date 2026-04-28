#!/bin/bash
# digest-parallel.sh — Drive parallel ingestion of ~/.claude/projects/*.jsonl
#
# Two phases:
#   1. xargs -P N runs digest-worker.sh per file (claudefast call + staging JSON)
#   2. Serial jq merge of staging/*.json into ~/.claude-team/insights/index.json
#
# Usage:
#   digest-parallel.sh [--days N] [--max-files N] [--concurrency N]
#
# Defaults: days=7, max-files=∞, concurrency=16.
# Idempotent: skips files already in .processed-files. Re-running is safe.

set -u

DAYS=7
MAX_FILES=999999
CONCURRENCY=16

while [[ $# -gt 0 ]]; do
  case "$1" in
    --days) DAYS="$2"; shift 2 ;;
    --max-files) MAX_FILES="$2"; shift 2 ;;
    --concurrency|-j) CONCURRENCY="$2"; shift 2 ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

TEAM_DIR="${HOME}/.claude-team"
PROJECTS_DIR="${HOME}/.claude/projects"
STAGING_DIR="${TEAM_DIR}/insights/staging"
INDEX="${TEAM_DIR}/insights/index.json"
LOG_DIR="${TEAM_DIR}/logs"
LOG="${LOG_DIR}/digest-parallel-$(date +%Y%m%d-%H%M%S).log"
PROCESSED_LIST="${TEAM_DIR}/.processed-files"
WORKER="$(dirname "$0")/digest-worker.sh"

[[ ! -x "${WORKER}" ]] && { echo "worker missing/not-executable: ${WORKER}" >&2; exit 2; }
mkdir -p "${STAGING_DIR}" "${LOG_DIR}"
touch "${PROCESSED_LIST}"
[[ ! -s "${INDEX}" ]] && echo '{"insights":[]}' > "${INDEX}"

UPLOADER=$(whoami)
UPLOADER_IP=$(hostname)

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "${LOG}"; }
log "start days=${DAYS} max=${MAX_FILES} j=${CONCURRENCY} index=${INDEX}"

# 1. Build candidate list (newest first, skip already-processed).
FILE_LIST=$(mktemp)
PROCESSED_SORTED=$(mktemp)
CANDIDATES=$(mktemp)
trap 'rm -f "${FILE_LIST}" "${PROCESSED_SORTED}" "${CANDIDATES}" "${INDEX}.tmp" 2>/dev/null' EXIT

find "${PROJECTS_DIR}" -name "*.jsonl" -type f -mtime "-${DAYS}" -size +2k 2>/dev/null \
  | xargs -I{} stat -f '%m %N' {} 2>/dev/null \
  | sort -rn | awk '{$1=""; sub(/^ /,""); print}' > "${FILE_LIST}"

sort -u "${PROCESSED_LIST}" > "${PROCESSED_SORTED}"
sort "${FILE_LIST}" | comm -23 - "${PROCESSED_SORTED}" > "${CANDIDATES}"
TOTAL_CANDIDATES=$(wc -l < "${FILE_LIST}" | tr -d ' ')
TODO=$(wc -l < "${CANDIDATES}" | tr -d ' ')
log "candidates: ${TOTAL_CANDIDATES} total, ${TODO} todo (after dedup)"

if [[ "${MAX_FILES}" != "999999" ]]; then
  head -n "${MAX_FILES}" "${CANDIDATES}" > "${CANDIDATES}.cap" && mv "${CANDIDATES}.cap" "${CANDIDATES}"
  TODO=$(wc -l < "${CANDIDATES}" | tr -d ' ')
  log "capped to ${TODO} files"
fi

if [[ "${TODO}" == "0" ]]; then
  log "nothing to do"
  exit 0
fi

# 2. Phase 1: parallel workers.
START=$(date +%s)
log "phase1: spawning ${CONCURRENCY} workers"

# xargs -P N -n 1 — one file per worker invocation.
# Worker writes its result to staging/ + appends to processed-files.
xargs -P "${CONCURRENCY}" -n 1 -I{} "${WORKER}" {} < "${CANDIDATES}" 2>>"${LOG}"
RC=$?
ELAPSED=$(($(date +%s) - START))
log "phase1: done rc=${RC} elapsed=${ELAPSED}s"

# 3. Phase 2: serial merge of staging files into index.json.
log "phase2: merging staging files into index"

# Collect every staged insight, dedupe by content_hash, append non-duplicates.
STAGE_COUNT=$(find "${STAGING_DIR}" -name '*.json' -type f 2>/dev/null | wc -l | tr -d ' ')
log "phase2: ${STAGE_COUNT} staging files"

if [[ "${STAGE_COUNT}" == "0" ]]; then
  log "phase2: nothing to merge"
  exit 0
fi

NOW_TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Single jq pass: read existing index, fold every staging file into it, dedupe by content_hash.
# content_hash = sha256(name|when_to_use|description) — computed via jq's @sh + tostring is awkward;
# we compute outside jq using a per-file shell loop into one consolidated stream, then jq merges.

CONSOLIDATED=$(mktemp)
trap 'rm -f "${FILE_LIST}" "${PROCESSED_SORTED}" "${CANDIDATES}" "${INDEX}.tmp" "${CONSOLIDATED}" 2>/dev/null' EXIT

find "${STAGING_DIR}" -name '*.json' -type f 2>/dev/null | while read -r sf; do
  jq -c '.[]?' "${sf}" 2>/dev/null
done | while IFS= read -r insight; do
  NAME=$(printf '%s' "${insight}" | jq -r '.name')
  WHEN=$(printf '%s' "${insight}" | jq -r '.when_to_use')
  DESC=$(printf '%s' "${insight}" | jq -r '.description')
  RAW_HASH=$(printf '%s' "${insight}" | jq -r '.raw_hash')
  SRC=$(printf '%s' "${insight}" | jq -r '.source')
  CONTENT_HASH=$(printf '%s|%s|%s' "${NAME}" "${WHEN}" "${DESC}" | shasum -a 256 | awk '{print $1}')
  jq -n --arg n "${NAME}" --arg u "${UPLOADER}" --arg ip "${UPLOADER_IP}" \
        --arg w "${WHEN}" --arg d "${DESC}" --arg ch "${CONTENT_HASH}" \
        --arg rh "${RAW_HASH}" --arg t "${NOW_TS}" --arg src "${SRC}" \
     '{name:$n, uploader:$u, uploader_ip:$ip, when_to_use:$w, description:$d,
       content_hash:$ch, raw_hashes:[$rh], source:$src, created_at:$t}'
done > "${CONSOLIDATED}"

NEW_LINES=$(wc -l < "${CONSOLIDATED}" | tr -d ' ')
log "phase2: ${NEW_LINES} candidate insight rows"

# Merge: existing.insights + new (deduped by content_hash, keeping first).
jq -s --slurpfile idx "${INDEX}" '
   $idx[0] as $existing
   | ($existing.insights | map(.content_hash)) as $known
   | {insights: ($existing.insights + (
       . | unique_by(.content_hash) | map(select(.content_hash as $ch | $known | index($ch) | not))
     ))}
' "${CONSOLIDATED}" > "${INDEX}.tmp" && mv "${INDEX}.tmp" "${INDEX}"

FINAL=$(jq '.insights | length' "${INDEX}")
log "phase2: index now has ${FINAL} insights"

# 4. Move staged files into archive (so re-runs only merge new staging).
ARCHIVE_DIR="${TEAM_DIR}/insights/staging-archive/$(date +%Y%m%d)"
mkdir -p "${ARCHIVE_DIR}"
mv "${STAGING_DIR}"/*.json "${ARCHIVE_DIR}/" 2>/dev/null || true
log "phase2: moved staging files to ${ARCHIVE_DIR}"

log "complete"
