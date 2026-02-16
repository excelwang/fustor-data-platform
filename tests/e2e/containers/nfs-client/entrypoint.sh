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

# Start sensord if enabled
if [ "${AGENT_ENABLED}" = "true" ]; then
    echo "Starting Fustor sensord (${AGENT_ID})..."
    
    # Create sensord config directory
    mkdir -p /root/.fustor/sensord-config
    
    # Copy and process config template with environment variable substitution
    # The config file is mounted from tests/e2e/config/sensord-config/default.yaml
    if [ -f "/config/sensord-config/default.yaml" ]; then
        # Substitute environment variables in the config
        # Uses gettext-base (envsubst) installed in Dockerfile
        envsubst < /config/sensord-config/default.yaml > /root/.fustor/sensord-config/default.yaml
        echo "sensord config loaded and processed from mounted volume"
    else
        echo "ERROR: sensord config file not found at /config/sensord-config/default.yaml"
        exit 1
    fi
    
    # Start sensord in foreground
    echo "Starting Fustor sensord (${AGENT_ID}) in foreground..."
    exec sensord start
fi

# Keep container running if sensord was not started
echo "Container ready. Entering idle loop..."
exec tail -f /dev/null
