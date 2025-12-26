#!/bin/bash

# Script to mount network share
# Usage: ./mount-network.sh

MOUNT_POINT="/mnt/pdfs"
SHARE_PATH="//172.31.3.171/SharedData"
USERNAME="Administrator"
DOMAIN="WORKGROUP"
PASSWORD="7JxSwzcJ%2!sW*fmL73z-hDrcgs4QlIi@123"

# Create mount point if it doesn't exist
if [ ! -d "$MOUNT_POINT" ]; then
    echo "Creating mount point: $MOUNT_POINT"
    sudo mkdir -p "$MOUNT_POINT"
fi

# Check if already mounted
if mountpoint -q "$MOUNT_POINT"; then
    echo "⚠️  $MOUNT_POINT is already mounted"
    echo "To unmount: sudo umount $MOUNT_POINT"
    exit 0
fi

# Mount the network share
echo "Mounting $SHARE_PATH to $MOUNT_POINT..."
sudo mount -t cifs "$SHARE_PATH" "$MOUNT_POINT" \
    -o username="$USERNAME",domain="$DOMAIN",password="$PASSWORD",vers=3.0,sec=ntlmssp

# Check if mount was successful
if [ $? -eq 0 ]; then
    echo "✅ Successfully mounted to $MOUNT_POINT"
    echo "📁 Contents:"
    ls -la "$MOUNT_POINT" | head -10
else
    echo "❌ Mount failed!"
    exit 1
fi

