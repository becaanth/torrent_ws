#!/bin/bash

# Check if Run ID argument was passed
if [ -z "$1" ]; then
    echo "Error: Please provide a Run ID."
    echo "Usage: $0 <run_id> [posegraph_name]"
    exit 1
fi

RUN_ID=$1
# Use $2 if provided, otherwise default to 'offline40'
GRAPH_NAME="${2:-offline40}"

SCRIPT_PATH="$VTRROOT/vtr3_pose_graph/samples/plot_teach_path.py"
GRAPH_PATH="$VTRTEMP/pgs/${GRAPH_NAME}/graph/"

echo "Starting plotter loop for Run ID: $RUN_ID (Graph: $GRAPH_NAME). Press [CTRL+C] to stop."
echo "Target Graph: $GRAPH_PATH"
echo "--------------------------------------------------------"

while true; do
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Executing VTR Plotter..."
    
    python3 "$SCRIPT_PATH" -g "$GRAPH_PATH"
    
    echo "Waiting 5 seconds..."
    echo "--------------------------------------------------------"
    sleep 5
done