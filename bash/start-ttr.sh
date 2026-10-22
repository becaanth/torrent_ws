#!/usr/bin/env bash
# NOTE: COPY TO HOME DIRECTORY ON ROBOTS
# Start a Teach, Torrent, Repeat robot in a detached tmux session.
#
# Run on the ROBOT HOST (outside docker):   ./start_ttr.sh <teacher|repeater> <posegraph> [policy]
# From your laptop:                          ssh <robot> '~/start_ttr.sh repeater <posegraph> sequential'
# policy defaults to rarest-random (orchestrator.py -r)
# Returns immediately; everything keeps running after you disconnect.
#
# To watch later:
#   ssh -t <robot> docker exec -it vtr tmux attach -t ttr
# To stop:
#   ssh <robot> docker exec vtr tmux kill-session -t ttr
set -e

USAGE="usage: start_ttr.sh <teacher|repeater> <posegraph> [policy]"
ROLE=${1:?$USAGE}
PG=${2:?$USAGE}
POLICY=${3:-rarest-random}   # rarest-random | sequential | cascading | hybrid | sequence-random

case "$POLICY" in
  rarest-random|sequential|cascading|hybrid|sequence-random) ;;
  *) echo "unknown policy '$POLICY'" >&2; echo "$USAGE" >&2; exit 1 ;;
esac

CONTAINER=vtr3_ttr                                     # <-- your container name
SCRIPTS=/home/indro/ASRL/vtr3/torrent_ws/scripts       # path INSIDE the container

case "$ROLE" in
  teacher)  YAML=$SCRIPTS/ttr_teacher.launch.yaml ;;   # VTR + orchestrator
  repeater) YAML=$SCRIPTS/ttr_repeater.launch.yaml ;;  # VTR + orchestrator + torrent_planner
  *) echo "role must be 'teacher' or 'repeater', got '$ROLE'" >&2; exit 1 ;;
esac

# 1. make sure the container is running (no-op if it already is)
if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != "true" ]; then
  docker start "$CONTAINER" >/dev/null   # if you normally create it with `docker run ...`, put that here instead
fi

# 2. inside the container: kill any old session, load the new one detached.
#    bash -i so ~/.bashrc runs and VTRSRC / VTRTEMP / VTRROOT / ROBOT_NAME are set
#    (tmuxp expands ${VTRTEMP} etc. at load time, so they must exist here).
docker exec -e TTR_POSEGRAPH="$PG" -e TTR_POLICY="$POLICY" "$CONTAINER" bash -ic "
  tmux kill-session -t ttr 2>/dev/null || true
  tmuxp load -d '$YAML'
"

echo "started $ROLE session 'ttr' in container '$CONTAINER' with posegraph '$PG', policy '$POLICY'"