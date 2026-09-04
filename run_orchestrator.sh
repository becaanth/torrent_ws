#!/bin/bash

# Check for required arguments
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <posegraph> <method>"
    exit 1
fi

# Pass arguments directly into tmuxp
POSEGRAPH="$1" METHOD="$2" tmuxp load experiment_config.yaml