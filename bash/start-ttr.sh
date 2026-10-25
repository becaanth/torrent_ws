#!/usr/bin/env bash
# Start a Teach, Torrent, Repeat robot in a detached tmux session.
#
# Run on the ROBOT HOST (outside docker):   ./start_ttr.sh <teacher|repeater> <posegraph> [policy]
# From your laptop:                          ssh <robot> '~/start_ttr.sh repeater <posegraph> sequential'
# policy defaults to rarest-random (orchestrator.py -r)
# Returns immediately; everything keeps running after you disconnect.
#
# To watch later:
#   ssh -t <robot> docker exec -it vtr3_ttr tmux attach -t ttr
# To stop:
#   ssh <robot> docker exec vtr3_ttr tmux kill-session -t ttr
set -e

USAGE="usage: start_ttr.sh <teacher|repeater> <posegraph> [policy]"
ROLE=${1:?$USAGE}
PG=${2:?$USAGE}
POLICY=${3:-rarest-random}   # rarest-random | sequential | cascading | hybrid | sequence-random

case "$POLICY" in
  rarest-random|sequential|cascading|hybrid|sequence-random) ;;
  *) echo "unknown policy '$POLICY'" >&2; echo "$USAGE" >&2; exit 1 ;;
esac

case "$ROLE" in
  teacher)  YAML_NAME=ttr_teacher.launch.yaml ;;   # VTR + orchestrator
  mteacher)  YAML_NAME=ttr_mteacher.launch.yaml ;;   # VTR + orchestrator
  repeater) YAML_NAME=ttr_repeater.launch.yaml ;;  # VTR + orchestrator + torrent_planner
  *) echo "role must be 'teacher' or 'repeater', got '$ROLE'" >&2; exit 1 ;;
esac

CONTAINER=vtr3_ttr

# 1. make sure the container is running (same as your manual `docker start vtr3_ttr`)
if [ "$(docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" != "true" ]; then
  docker start "$CONTAINER" >/dev/null
fi

# 2. inside the container, through entrypoint.sh (same as your manual
#    `docker exec -it vtr3_ttr entrypoint.sh bash`, minus -it: no terminal needed).
#    entrypoint.sh sets up the environment (VTRSRC, VTRTEMP, ROBOT_NAME, ...), then runs bash -c.
#    The tmux server started here inherits that environment, so every pane has it too.
#
#    - kill-server: fresh tmux server each launch, so panes never inherit a stale
#      environment from an old server. Kills ANY tmux session in this container.
#    - COLUMNS/LINES: tmuxp creates the session at the size of the terminal it runs in
#      (shutil.get_terminal_size). With no terminal that falls back to 80x24, which is
#      too small to split the 4-pane vtr_gui window ("no space for new pane").
#      get_terminal_size reads COLUMNS/LINES first, so setting them gives a 250x60 session.
#    - default-size: same size for any session tmux itself creates without a client.
#    - \$HOME is expanded inside the container (root -> /root).
docker exec -e TTR_POSEGRAPH="$PG" -e TTR_POLICY="$POLICY" "$CONTAINER" entrypoint.sh bash -c "
  tmux kill-server 2>/dev/null || true
  grep -qs 'default-size' \$HOME/.tmux.conf || echo 'set -g default-size 250x60' >> \$HOME/.tmux.conf
  COLUMNS=250 LINES=60 tmuxp load -d \$HOME/ASRL/vtr3/torrent_ws/scripts/$YAML_NAME
"

echo "started $ROLE session 'ttr' in container '$CONTAINER' with posegraph '$PG', policy '$POLICY'"