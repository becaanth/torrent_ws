#!/bin/bash

# 1. Check if an integer argument was passed
if [ -z "$1" ]; then
    echo "Error: Please provide an integer argument (Run ID)."
    echo "Usage: $0 <integer_id>"
    exit 1
fi

RUN_ID=$1

# 2. Define path constants based on your environment variables
SCRIPT_PATH="$VTRROOT/vtr3_pose_graph/samples/plot_teach_path.py"
GRAPH_PATH="$VTRTEMP/pgs/rdome_loop_$RUN_ID/graph/"

echo "Starting plotter loop for Run ID: $RUN_ID. Press [CTRL+C] to stop."
echo "Target Graph: $GRAPH_PATH"
echo "--------------------------------------------------------"

while true; do
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] Executing VTR Plotter..."
    
    # 3. Call the VTR script with the graph directory and your integer ID flag
    python3 "$SCRIPT_PATH" -g "$GRAPH_PATH"
    
    echo "Waiting 5 seconds..."
    echo "--------------------------------------------------------"
    sleep 1
done