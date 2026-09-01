#!/usr/bin/env bash
# Idempotent Cloud Agent setup for the yaal repository.
# Installs Python build/venv tooling plus a nested-container-friendly Docker
# engine, builds the project's virtualenv, and pre-pulls the docker compose
# images used by the integration and .NET test suites.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# Keep existing conffiles (e.g. fuse.conf) so package installs stay
# non-interactive instead of stopping at a conffile prompt.
APT_INSTALL=(sudo apt-get install -y -qq -o Dpkg::Options::=--force-confold)

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Installing system packages"
sudo apt-get update -qq
"${APT_INSTALL[@]}" \
  python3-venv python3-dev build-essential \
  ca-certificates curl gnupg \
  fuse-overlayfs iptables uidmap

echo "==> Installing Docker CE (engine + compose plugin)"
if ! command -v docker >/dev/null 2>&1; then
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    | sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  . /etc/os-release
  echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -qq
  "${APT_INSTALL[@]}" \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> Configuring Docker for the nested-container VM"
# The default overlayfs snapshotter cannot mount inside this VM, so use the
# classic graph driver backed by fuse-overlayfs. A slightly reduced MTU avoids
# packet drops on the pod's overlay network.
sudo mkdir -p /etc/docker
echo '{
  "features": { "containerd-snapshotter": false },
  "storage-driver": "fuse-overlayfs",
  "mtu": 1400
}' | sudo tee /etc/docker/daemon.json >/dev/null
sudo usermod -aG docker "$USER" || true

echo "==> Creating Python virtualenv and installing yaal (editable)"
make install

echo "==> Pre-pulling docker compose images (best effort)"
# Bring Docker up the same way a fresh boot would, then warm the image cache so
# integration and .NET test runs do not have to download ~2.5GB on first use.
if bash "$REPO_ROOT/.cursor/start.sh"; then
  for img in \
    postgres:16-alpine \
    mysql:8.4 \
    clickhouse/clickhouse-server:24.8-alpine \
    mcr.microsoft.com/dotnet/sdk:8.0; do
    docker pull "$img" || echo "   (skipped pull of $img)"
  done
else
  echo "   Docker not available during install; images will pull on first use."
fi

echo "==> Install complete"
