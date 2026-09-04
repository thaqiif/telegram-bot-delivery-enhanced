#!/usr/bin/env bash
# Debian installer for telegram-bulk-delivery.
#
# Idempotent: safe to re-run. Existing config, master key, and env file are
# never overwritten.
#
# Run from inside an extracted release tarball:
#   sudo ./install.sh
set -euo pipefail

INSTALL_BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/telegram-bulk-delivery"
DATA_DIR="/var/lib/telegram-bulk-delivery"
SERVICE_USER="tgbulk"
BIN_NAME="telegram-bulk-delivery"

[ "$(id -u)" -eq 0 ] || { echo "error: run as root (sudo ./install.sh)"; exit 1; }
cd "$(dirname "$0")"
[ -x "./${BIN_NAME}" ] || { echo "error: ./${BIN_NAME} not found next to install.sh"; exit 1; }
command -v systemctl >/dev/null || { echo "error: systemctl not found (Debian only)"; exit 1; }

ARCH="$(uname -m)"
echo ">> installing ${BIN_NAME} (linux-${ARCH}) for Debian"

# 1) Binary
install -m 0755 "./${BIN_NAME}" "${INSTALL_BIN_DIR}/${BIN_NAME}"

# 2) Dedicated system user (no login)
if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
    adduser --system --group --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

# 3) Directories
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${CONFIG_DIR}"
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}"
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}/files"

# 4) Master key (encrypts bot tokens at rest — BACK THIS UP; losing it makes
#    all stored bot tokens unrecoverable).
if [ ! -s "${CONFIG_DIR}/master.key" ]; then
    umask 077
    openssl rand -base64 32 | tr -d '\n' > "${CONFIG_DIR}/master.key"
    echo ">> generated new master key at ${CONFIG_DIR}/master.key"
else
    echo ">> keeping existing master key"
fi
chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/master.key"
chmod 0400 "${CONFIG_DIR}/master.key"

# 5) Env file consumed by the systemd unit.
if [ ! -s "${CONFIG_DIR}/env" ]; then
    cat > "${CONFIG_DIR}/env" <<'EOF'
BULK_MASTER_KEY=<filled-by-install>
# Point at a Telegram test-environment rewrite proxy if you use a test bot:
#TELEGRAM_API_BASE=http://127.0.0.1:8443
EOF
    sed -i "s|<filled-by-install>|$(cat "${CONFIG_DIR}/master.key")|" "${CONFIG_DIR}/env"
    chmod 0640 "${CONFIG_DIR}/env"
    chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/env"
else
    echo ">> keeping existing env file"
fi

# 6) Operator config (absolute paths for the systemd deployment).
if [ ! -s "${CONFIG_DIR}/config.toml" ]; then
    cat > "${CONFIG_DIR}/config.toml" <<EOF
database_path = "${DATA_DIR}/delivery.db"
bind = "0.0.0.0:8080"
key_id = "v1"
writer_queue_capacity = 128
reader_queue_capacity = 64
max_concurrent_http = 64
max_telegram_inflight = 32
max_webhook_inflight = 4
max_blocking_threads = 8
max_recipients_per_job = 100000
global_nonterminal_recipients = 500000
request_body_bytes = 33554432
multipart_body_bytes = 67108864
shared_parameters_bytes = 65536
recipient_patch_bytes = 8192
free_disk_reserve_bytes = 1073741824
storage_high_watermark_bytes = 5368709120
wal_truncate_bytes = 67108864
retention_sweep_secs = 30
retention_batch = 500
EOF
    chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/config.toml"
    echo ">> wrote ${CONFIG_DIR}/config.toml (listen 0.0.0.0:8080)"
else
    echo ">> keeping existing config.toml"
fi

# 7) systemd unit
install -m 0644 ./telegram-bulk-delivery.service /etc/systemd/system/telegram-bulk-delivery.service
systemctl daemon-reload
systemctl enable telegram-bulk-delivery.service >/dev/null

echo
echo "== installed =="
echo "  binary:   ${INSTALL_BIN_DIR}/${BIN_NAME}"
echo "  config:   ${CONFIG_DIR}/config.toml"
echo "  env:      ${CONFIG_DIR}/env"
echo "  data:     ${DATA_DIR}"
echo "  key:      ${CONFIG_DIR}/master.key  (BACK THIS UP)"
echo
echo "start with:   systemctl start telegram-bulk-delivery"
echo "check with:   curl -s http://127.0.0.1:8080/healthz"
echo
echo "The service listens on 0.0.0.0:8080 by default; put an nginx/caddy"
echo "TLS proxy in front for public exposure (see README)."
