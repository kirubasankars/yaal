#!/usr/bin/env bash
# Per-boot Docker bring-up for the yaal Cloud Agent environment.
# Safe to run repeatedly: it starts the daemon only when needed, applies the
# nested-container networking fix, and returns once Docker is reachable.
set -euo pipefail

# The outer pod ships an iptables-legacy FORWARD policy of DROP. Docker's real
# rules live in the nft backend, so that legacy DROP silently blocks egress from
# docker compose (user-defined bridge) networks while leaving the default bridge
# working. Relax the legacy policy; nft still provides Docker's isolation/NAT.
sudo iptables-legacy -P FORWARD ACCEPT 2>/dev/null || true

if ! sudo docker info >/dev/null 2>&1; then
  echo "==> Starting Docker daemon"
  sudo service docker start || true
  for _ in $(seq 1 30); do
    sudo docker info >/dev/null 2>&1 && break
    sleep 1
  done
fi

# Allow the non-root agent user to reach the daemon without sudo (the socket's
# group ownership resets on each daemon start).
sudo chmod 666 /var/run/docker.sock 2>/dev/null || true

if sudo docker info >/dev/null 2>&1; then
  echo "==> Docker ready"
else
  echo "==> Docker failed to start" >&2
  exit 1
fi
