#!/bin/bash
case "$OSTYPE" in
  linux-gnu*)
    ##########################################################################
    # Proxmox server
    ##########################################################################
    cd /root/proxmox-stats

    # Load the Python virtual environment
    source venv/bin/activate

    # Load .env variables
    export $(grep -v '^#' .env | xargs)

    # Run the script
    python proxmox-stats-to-mqtt.py
    ;;

  msys)
    ##########################################################################
    # Windows, running in Git Bash
    ##########################################################################
    #source venv/bin/activate
    source .env
    python proxmox-stats-to-mqtt.py
    ;;
esac