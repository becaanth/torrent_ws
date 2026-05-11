#!/bin/bash

# 1. Define your 10 bag paths manually here
# Add or remove paths inside the parentheses (space-separated)
export VTRTEMP='/home/asrl/ASRL/vtr3/temp'
BAGS=(
    # "nanook1"
    "nanook2"
    # "nanook3"
    "nanook4"
    # "nanook5"
    # "woody_convoy"
    # "virtr_urban"
    "test_indoors"
    # "newnicycle"
)

# 2. Start the monitor script first
# We use & to run it in the background so the script can continue
cd $VTRROOT/torrent_ws/posegraph
python3 rpe_viz.py &

echo "Monitor started. Launching ${#BAGS[@]} creation instances..."
mkdir -p logs

# 3. Loop through the array and launch the python scripts
for BAG in "${BAGS[@]}"
do
    # Extract the base name of the folder to create a clean log filename
    LOG_NAME=$(basename "$BAG")
    
    echo "Processing: $BAG"
    
    # Run creation script in background
    python3 deconstruct_posegraph.py -b "$BAG" > "logs/${LOG_NAME}.log" 2>&1 &
    python3 deconstruct_posegraph.py -b "$BAG" -t True > "logs/${LOG_NAME}.log" 2>&1 &
done

echo "-------------------------------------------------------"
echo "All instances are running in the background."
echo "Use 'top' or 'ps aux | grep python' to see activity."
echo "-------------------------------------------------------"

# Wait for all background jobs to finish
wait