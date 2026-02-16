#!/bin/bash
set -e

# Wait for NFS server to be ready
echo "Waiting for NFS server at ${NFS_SERVER}:${NFS_PATH}..."
until showmount -e "${NFS_SERVER}" 2>/dev/null | grep -q "${NFS_PATH}"; do
    sleep 1
done
echo "NFS server is ready."

# Mount NFS share
echo "Mounting NFS share..."
mount -t nfs -o "${MOUNT_OPTIONS}" "${NFS_SERVER}:${NFS_PATH}" "${MOUNT_POINT}"
echo "NFS mounted at ${MOUNT_POINT}"

# Start datacastd if enabled
if [ "${AGENT_ENABLED}" = "true" ]; then
    echo "Starting Fustor datacastd (${AGENT_ID})..."
    
    # Create datacastd configuration
    mkdir -p /data/datacastd
    cat > /data/datacastd/config.yaml << EOF
datacastd:
  id: "${AGENT_ID}"
  host: "0.0.0.0"
  port: ${AGENT_PORT}
  data_dir: "/data/datacastd"

sources:
  shared-fs:
    driver: "source-fs"
    config:
      watch_paths:
        - "${MOUNT_POINT}"
      scan_interval: 60
      audit_interval: 300

senders:
  fustord:
    driver: "fustord"
    config:
      endpoint: "${FUSION_ENDPOINT}"
      credential:
        key: "${API_KEY}"
      view_id: "${VIEW_ID}"
EOF

    # Start datacastd in background
    datacastd start --config /data/datacastd/config.yaml &
    AGENT_PID=$!
    echo "datacastd started with PID ${AGENT_PID}"
fi

# Keep container running
echo "Container ready. Entering idle loop..."
tail -f /dev/null
