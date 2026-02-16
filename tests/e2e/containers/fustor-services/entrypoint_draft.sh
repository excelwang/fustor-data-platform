#!/bin/bash
set -e

# Configuration 
CONFIG_DIR="/root/.fustor/fustord-config"

echo "Processing fustord configuration templates..."

# Process all YAML files in the config directory if they exist
if [ -d "$CONFIG_DIR" ]; then
    for template in "$CONFIG_DIR"/*.yaml; do
        if [ -f "$template" ]; then
            echo "Substituting environment variables in $template"
            # We use a temp file to avoid issues with reading/writing same file
            # or we can rely on envsubst behavior (usually fine if redirects are handled by shell)
            # But here we are processing mounted files. If they are read-only mounts, we can't write back.
            # Docker binds are read-write by default. 
            # However, modifying the file on the host (tests/e2e/config/fustord-config/default.yaml) is NOT what we want!
            # We want to modify the file inside the container, but if it's a bind mount, it modifies the host file.
            
            # CRITICAL: If we modify bind-mounted file, we change the source code!
            # We must NOT modify the mounted file in place if it's mapped to source.
            
            # Solution: Copy config files from mount to a working directory, process them, 
            # and verify fustord uses the working directory.
            # fustord uses FUSTOR_HOME/fustord-config.
            
            # If FUSTOR_HOME is /root/.fustor
            # And we mount to /root/.fustor/fustord-config
            # Then we are stuck.
            
            # Alternative: Mount to /config/fustord-config (read-only), 
            # copy to /root/.fustor/fustord-config, then process.
        fi
    done
fi

# We need to change how we mount in docker-compose.yml first.
# Current mount: ./config/fustord-config:/root/.fustor/fustord-config
# This is dangerous for envsubst.
