#!/usr/bin/env bash
# Deploy to a Linux VM over SSH with Docker Compose.
#
#   DEPLOY_HOST=root@203.0.113.10 scripts/deploy.sh --bootstrap   # first time: installs Docker
#   DEPLOY_HOST=root@203.0.113.10 scripts/deploy.sh               # ship HEAD and restart
#
# Ships the committed tree (git archive HEAD), so commit before deploying.
# Secrets are generated on the VM into $DEPLOY_DIR/.env on the first deploy
# and never leave it; nothing secret is in the repo or on this machine.
set -euo pipefail

: "${DEPLOY_HOST:?set DEPLOY_HOST, e.g. DEPLOY_HOST=root@203.0.113.10}"
DEPLOY_DIR="${DEPLOY_DIR:-/opt/taskqueue}"
cd "$(dirname "$0")/.."

if [[ -n "$(git status --porcelain)" ]]; then
  echo "warning: uncommitted changes are not deployed (shipping $(git rev-parse --short HEAD))" >&2
fi

if [[ "${1:-}" == "--bootstrap" ]]; then
  echo "==> installing Docker on $DEPLOY_HOST"
  ssh "$DEPLOY_HOST" 'command -v docker >/dev/null || curl -fsSL https://get.docker.com | sh'
fi

echo "==> shipping $(git rev-parse --short HEAD) to $DEPLOY_HOST:$DEPLOY_DIR"
git archive --format=tar HEAD | ssh "$DEPLOY_HOST" "mkdir -p '$DEPLOY_DIR' && tar -x -C '$DEPLOY_DIR'"

echo "==> starting the stack"
ssh "$DEPLOY_HOST" "DEPLOY_DIR='$DEPLOY_DIR' bash -s" <<'REMOTE'
set -euo pipefail
cd "$DEPLOY_DIR"
if [ ! -f .env ]; then
  umask 077
  gen() { openssl rand -hex 24; }
  cat > .env <<ENV
MYSQL_DATABASE=taskqueue
MYSQL_USER=taskqueue
MYSQL_PASSWORD=$(gen)
MYSQL_ROOT_PASSWORD=$(gen)
GRAFANA_ADMIN_PASSWORD=$(gen)
API_KEY=$(gen)
ENV
  echo "created $DEPLOY_DIR/.env with fresh secrets"
fi
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build --remove-orphans \
  --wait --wait-timeout 300 api worker scheduler prometheus grafana
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps --format 'table {{.Service}}\t{{.Status}}'
REMOTE

HOST="${DEPLOY_HOST#*@}"
echo
echo "==> live: http://$HOST/health  (docs: http://$HOST/docs)"
echo "    API key: ssh $DEPLOY_HOST grep API_KEY $DEPLOY_DIR/.env"
