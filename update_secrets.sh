#!/usr/bin/env bash
# Usage:
#   sh update_repo_secrets.sh --from-file ./secrets.env
#   sh update_repo_secrets.sh NAME1=VALUE1 NAME2=VALUE2 NAME3=VALUE3 NAME4=VALUE4 NAME5=VALUE5
#
# Install gh: brew install gh
# First time auth: gh auth login
set -euo pipefail

ORG="${ORG:-e-laute}"

declare -A SECRETS
declare -A overwrite_decisions
SECRET_FILE=""

usage() {
  echo "Usage:"
  echo "  $0 --from-file ./secrets.env"
  echo "  $0 NAME1=VALUE1 NAME2=VALUE2 ..."
  echo
  echo "Env:"
  echo "  ORG=<github-org>   (default: e-laute)"
}

abort() { echo "Error: $*" >&2; exit 1; }

require_gh() {
  command -v gh >/dev/null 2>&1 || abort "gh CLI is not installed."
  gh auth status >/dev/null 2>&1 || abort "gh CLI not authenticated. Run: gh auth login"
}

parse_kv() {
  local kv="$1"
  [[ "$kv" == *"="* ]] || abort "Invalid secret spec '$kv' (expected NAME=VALUE)"
  local name="${kv%%=*}"
  local value="${kv#*=}"
  [[ -n "$name" && -n "$value" ]] || abort "Empty name or value in '$kv'"
  SECRETS["$name"]="$value"
}

load_env_file() {
  local file="$1"
  [[ -f "$file" ]] || abort "Secret file not found: $file"
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"
    [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
    parse_kv "$line"
  done < "$file"
}

if [[ $# -eq 0 ]]; then usage; exit 1; fi
while (( "$#" )); do
  case "$1" in
    --from-file) shift; SECRET_FILE="$1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) parse_kv "$1"; shift ;;
  esac
done
[[ -n "$SECRET_FILE" ]] && load_env_file "$SECRET_FILE"

require_gh

# Repo loop - store repos in array to avoid pipe subshell issue
repos=()
while IFS=$'\t' read -r repo rest; do
  repos+=("$repo")
done < <(gh repo list "$ORG" --limit 1000 --no-archived --source)

for repo in "${repos[@]}"; do
  # Skip if repo matches exclusions
  if [[ "$repo" == "$ORG/ARCHIVED"* ]] || \
     [[ "$repo" == "$ORG/$ORG.github.io" ]] || \
     [[ "$repo" == "$ORG/SPARQL" ]] || \
     [[ "$repo" == "$ORG/extra" ]] || \
     [[ "$repo" == "$ORG/Database-" ]] || \
     [[ "$repo" == "$ORG/DATABASE_TabHum" ]] || \
     [[ "$repo" == "$ORG/DATABASE-README" ]] || \
     [[ "$repo" == "$ORG/GH_Actions" ]] || \
     [[ "$repo" == "$ORG/.github-private" ]] || \
     [[ "$repo" == "$ORG/scripts" ]] || \
     [[ "$repo" == "$ORG/audio" ]] || \
     [[ "$repo" == "$ORG/validate_encodings" ]] || \
     [[ "$repo" == "$ORG/upload_mei_to_TU-RDM" ]] || \
     [[ "$repo" == "$ORG/.github" ]]; then
    echo "Skipping repository: $repo"
    continue
  fi

  echo "Updating secrets in: $repo"

  # Get currently set secrets
  existing=$(gh secret list -R "$repo" | awk '{print $1}')

  for name in "${!SECRETS[@]}"; do
    if echo "$existing" | grep -q "^$name$"; then
      # Check if we already asked about this secret
      if [[ -z "${overwrite_decisions[$name]:-}" ]]; then
        read -p "Secret '$name' already exists in some repositories. Overwrite in all repos? (y/N) " ans
        case "$ans" in
          [yY]*) overwrite_decisions["$name"]="yes" ;;
          *) overwrite_decisions["$name"]="no" ;;
        esac
      fi
      
      if [[ "${overwrite_decisions[$name]}" != "yes" ]]; then
        echo "  Skipping $name"
        continue
      fi
    fi
    if gh secret set "$name" -R "$repo" --body "${SECRETS[$name]}" >/dev/null; then
      echo "  ✓ $name"
    else
      echo "  ✗ $name (failed)" >&2
    fi
  done
done
