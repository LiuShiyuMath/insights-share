#!/bin/bash
# classify-uncategorized.sh — Use claudefast to assign topic_slug to
# entries currently labeled "uncategorized". Batches of 8 per LLM call.
#
# Usage:
#   scripts/classify-uncategorized.sh                # dry-run, write mapping to /tmp
#   scripts/classify-uncategorized.sh --apply        # apply mapping to index.json
#
# Idempotent: only touches entries with topic_slug == "uncategorized".

set -e

INDEX="${HOME}/.claude-team/insights/index.json"
WORK_DIR="${HOME}/.claude-team/cache/classify"
MAPPING="${WORK_DIR}/mapping.tsv"
APPLY=0

if [[ "${1:-}" == "--apply" ]]; then APPLY=1; fi

mkdir -p "${WORK_DIR}"
: > "${MAPPING}"

EXISTING_TAXONOMY="jsonl-ingestion, claude-code-plugin, claude-code-hooks, claudefast-runtime, rules-memory, agent-orchestration, testing, git-workflow, rsync-sync, async-python, mcp-tools, security"

# Pull all uncategorized as compact records: hash<TAB>name<TAB>when<TAB>desc
RECORDS=$(jq -r '
  .insights
  | map(select(.topic_slug == "uncategorized"))
  | .[]
  | [.content_hash, (.name // ""), (.when_to_use // ""), (.description // "")]
  | @tsv
' "${INDEX}")

TOTAL=$(echo "${RECORDS}" | grep -c . || true)
echo "[classify] ${TOTAL} uncategorized entries to process (batches of 8)"

if [[ "${TOTAL}" -eq 0 ]]; then
  echo "[classify] nothing to do"
  exit 0
fi

BATCH_NUM=0
BATCH_SIZE=8
LINE_NUM=0
BATCH_HASHES=()
BATCH_PROMPT=""

flush_batch() {
  if [[ ${#BATCH_HASHES[@]} -eq 0 ]]; then return; fi
  BATCH_NUM=$((BATCH_NUM + 1))

  local prompt
  prompt=$(printf 'You are a topic classifier for Claude Code insights.\n\nFor each numbered item below, output ONE topic_slug (lowercase-hyphen-case).\n\nPrefer these existing slugs when applicable: %s.\nOnly invent a new slug if NONE fit.\n\nOutput ONLY a JSON array of %d strings, in the same order, no prose, no markdown.\n\nItems:\n%s\n\nOutput:' \
    "${EXISTING_TAXONOMY}" "${#BATCH_HASHES[@]}" "${BATCH_PROMPT}")

  local response
  response=$(claudefast -p "${prompt}" 2>/dev/null || echo "")

  # Strip markdown fences if any
  response=$(echo "${response}" | sed -e 's/^```json//' -e 's/^```//' -e 's/```$//' | tr -d '\r')

  local slugs
  slugs=$(echo "${response}" | jq -r '.[]?' 2>/dev/null || echo "")

  if [[ -z "${slugs}" ]]; then
    echo "[classify] batch ${BATCH_NUM}: malformed response, skipping ${#BATCH_HASHES[@]} entries"
    BATCH_HASHES=()
    BATCH_PROMPT=""
    return
  fi

  local i=0
  while IFS= read -r slug; do
    [[ -z "${slug}" ]] && continue
    if [[ ${i} -lt ${#BATCH_HASHES[@]} ]]; then
      printf '%s\t%s\n' "${BATCH_HASHES[$i]}" "${slug}" >> "${MAPPING}"
    fi
    i=$((i + 1))
  done <<< "${slugs}"

  echo "[classify] batch ${BATCH_NUM}: ${i}/${#BATCH_HASHES[@]} classified"
  BATCH_HASHES=()
  BATCH_PROMPT=""
}

while IFS=$'\t' read -r hash name whenuse desc; do
  [[ -z "${hash}" ]] && continue
  LINE_NUM=$((LINE_NUM + 1))

  IDX=$((LINE_NUM - BATCH_NUM * BATCH_SIZE))
  BATCH_HASHES+=("${hash}")

  # Truncate description for prompt budget
  desc_short="${desc:0:200}"
  BATCH_PROMPT+="$(printf '[%d] name: %s | when: %s | desc: %s\n' "${IDX}" "${name}" "${whenuse}" "${desc_short}")"$'\n'

  if [[ ${#BATCH_HASHES[@]} -eq ${BATCH_SIZE} ]]; then
    flush_batch
  fi
done <<< "${RECORDS}"

flush_batch  # trailing partial batch

CLASSIFIED=$(wc -l < "${MAPPING}" | tr -d ' ')
echo
echo "[classify] mapping: ${MAPPING} (${CLASSIFIED}/${TOTAL} classified)"
echo "[classify] new slugs introduced (top 15):"
awk -F'\t' '{print $2}' "${MAPPING}" | sort | uniq -c | sort -rn | head -15

if [[ "${APPLY}" -eq 0 ]]; then
  echo
  echo "[classify] dry-run only. Re-run with --apply to write index.json."
  exit 0
fi

# Apply mapping to index.json
TS=$(date +%Y%m%d-%H%M%S)
BACKUP="${INDEX}.bak-classify-${TS}"
cp "${INDEX}" "${BACKUP}"

# Convert TSV → JSON object: {hash: slug}
MAPPING_JSON=$(awk -F'\t' 'BEGIN{print "{"} NR>1{printf ","} {gsub(/"/, "\\\"", $1); gsub(/"/, "\\\"", $2); printf "\"%s\":\"%s\"", $1, $2} END{print "}"}' "${MAPPING}")

echo "${MAPPING_JSON}" > "${WORK_DIR}/mapping.json"

jq --slurpfile m "${WORK_DIR}/mapping.json" '
  .insights |= map(
    if .topic_slug == "uncategorized" and ($m[0][.content_hash] // null) != null
    then .topic_slug = $m[0][.content_hash]
    else .
    end
  )
' "${INDEX}" > "${WORK_DIR}/index.new.json"

mv "${WORK_DIR}/index.new.json" "${INDEX}"
echo "[classify] wrote ${INDEX}"
echo "[classify] backup: ${BACKUP}"
echo
echo "[classify] new topic_slug distribution:"
jq -r '
  .insights
  | group_by(.topic_slug)
  | map({slug: .[0].topic_slug, count: length})
  | sort_by(-.count)
  | .[]
  | "\(.count)\t\(.slug)"
' "${INDEX}" | awk -F'\t' '{ printf "  %4d  %s\n", $1, $2 }'
