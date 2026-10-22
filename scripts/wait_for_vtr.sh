#!/usr/bin/env bash
# Block until VTR is up, i.e. something is publishing /$ROBOT_NAME/vtr/robot_state
# (the topic tr_listener subscribes to). Lives in torrent_ws/scripts/.
topic="/${ROBOT_NAME}/vtr/robot_state"
echo "[wait_for_vtr] waiting for a publisher on ${topic} ..."
until ros2 topic info "${topic}" 2>/dev/null | grep -q "Publisher count: [1-9]"; do
  sleep 2
done
# small margin for the graph load to finish after the publisher appears
sleep 5
echo "[wait_for_vtr] VTR is up"