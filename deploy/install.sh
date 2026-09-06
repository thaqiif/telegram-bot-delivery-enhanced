#!/usr/bin/env bash
# One-shot installer for telegram-bulk-delivery.
#
# Two ways to run:
#   curl -fsSL https://raw.githubusercontent.com/thaqiif/telegram-bot-delivery-enhanced/main/deploy/install.sh | bash
#   sudo ./install.sh                      # from inside an extracted release tarball
#
# Non-root with sudo installed: the plain `| bash` one-liner re-runs itself
# as root automatically (sudo prompts for your password).
#
# Idempotent & non-destructive: existing config, master key, and env file
# are NEVER overwritten. Re-running upgrades the binary and restarts the
# service only when the binary actually changed. Pin a version with:
#   TGBULK_VERSION=v0.2.3 curl -fsSL <url> | bash
set -euo pipefail

REPO="thaqiif/telegram-bot-delivery-enhanced"
SELF_URL="https://raw.githubusercontent.com/${REPO}/main/deploy/install.sh"

INSTALL_BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/telegram-bulk-delivery"
DATA_DIR="/var/lib/telegram-bulk-delivery"
SERVICE_USER="tgbulk"
SERVICE_NAME="telegram-bulk-delivery"
BIN_NAME="telegram-bulk-delivery"

# --- root elevation ------------------------------------------------------
if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then
        if [ -f "$0" ] && [ -r "$0" ] && [ ! -p /dev/stdin ]; then
            echo ">> re-running as root via sudo"
            exec sudo bash "$0" "$@"
        fi
        echo ">> re-running as root via sudo (re-fetching installer)"
        curl -fsSL "${TGBULK_SELF_URL:-$SELF_URL}" | sudo bash
        exit $?
    fi
    echo "error: root required; install sudo or run: curl -fsSL ${SELF_URL} | sudo bash" >&2
    exit 1
fi

command -v curl >/dev/null 2>&1 || { echo "error: curl required"; exit 1; }
command -v systemctl >/dev/null 2>&1 || { echo "error: systemctl not found (Linux/systemd only)"; exit 1; }
command -v sha256sum >/dev/null 2>&1 || { echo "error: sha256sum required"; exit 1; }

# --- source binary: bundled tarball vs self-fetch ------------------------
LOCAL_BIN=""
VERSION="${TGBULK_VERSION:-}"
if [ -x "./${BIN_NAME}" ] && [ -f "./${SERVICE_NAME}.service" ]; then
    LOCAL_BIN="$(pwd)/${BIN_NAME}"
    # The release tarball unpacks to telegram-bulk-delivery-<version>-linux-<arch>
    VERSION="$(basename "$(pwd)" | sed -E 's/^telegram-bulk-delivery-([0-9][0-9.]*).*/\1/')"
    echo ">> using bundled ${BIN_NAME} (release tarball mode)"
fi

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64 | amd64) TRI="x86_64" ;;
    aarch64 | arm64) TRI="aarch64" ;;
    *) echo "error: unsupported architecture ${ARCH}"; exit 1 ;;
esac
[ "$(uname -s)" = "Linux" ] || { echo "error: only Linux release binaries are published"; exit 1; }

WORK_DIR=""
cleanup() { [ -n "$WORK_DIR" ] && rm -rf "$WORK_DIR"; }
trap cleanup EXIT

if [ -z "$LOCAL_BIN" ]; then
    if [ -z "$VERSION" ]; then
        echo ">> resolving latest release from GitHub"
        VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep -o '"tag_name"[[:space:]]*:[[:space:]]*"[^"]*"' \
            | head -1 \
            | sed -E 's/.*"v?([^"]*)"/\1/')"
    fi
    VERSION="${VERSION#v}"
    [ -n "$VERSION" ] || { echo "error: could not resolve latest release version"; exit 1; }

    ASSET="telegram-bulk-delivery-${VERSION}-linux-${TRI}"
    ASSET_URL="https://github.com/${REPO}/releases/download/v${VERSION}/${ASSET}.tar.gz"

    WORK_DIR="$(mktemp -d)"
    echo ">> downloading ${ASSET} (v${VERSION})"
    curl -fsSL -o "${WORK_DIR}/${ASSET}.tar.gz"        "${ASSET_URL}"
    curl -fsSL -o "${WORK_DIR}/${ASSET}.tar.gz.sha256" "${ASSET_URL}.sha256"
    echo ">> verifying sha256"
    ( cd "${WORK_DIR}" && sha256sum -c "${ASSET}.tar.gz.sha256" )
    tar -xzf "${WORK_DIR}/${ASSET}.tar.gz" -C "${WORK_DIR}"
    LOCAL_BIN="${WORK_DIR}/${ASSET}/${BIN_NAME}"
    [ -x "$LOCAL_BIN" ] || { echo "error: ${BIN_NAME} missing from release asset"; exit 1; }
fi
echo ">> installing ${BIN_NAME} v${VERSION:-local} (linux-${TRI})"

# 1) Dedicated system user (no login)
if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
    adduser --system --group --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

# 2) Directories
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${CONFIG_DIR}"
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}"
install -d -m 0750 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}/files"

# 3) Master key (encrypts bot tokens at rest — BACK THIS UP; losing it makes
#    all stored bot tokens unrecoverable)
if [ ! -s "${CONFIG_DIR}/master.key" ]; then
    command -v openssl >/dev/null 2>&1 || { echo "error: openssl required for first install"; exit 1; }
    umask 077
    openssl rand -base64 32 | tr -d '\n' > "${CONFIG_DIR}/master.key"
    echo ">> generated new master key at ${CONFIG_DIR}/master.key"
else
    echo ">> keeping existing master key"
fi
chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/master.key"
chmod 0400 "${CONFIG_DIR}/master.key"

# 4) Env file consumed by the systemd unit
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

# 5) Operator config (absolute paths for the systemd deployment)
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
allow_private_targets = false
EOF
    chown "${SERVICE_USER}:${SERVICE_USER}" "${CONFIG_DIR}/config.toml"
    echo ">> wrote ${CONFIG_DIR}/config.toml (listen 0.0.0.0:8080)"
else
    echo ">> keeping existing config.toml"
fi

# 6) systemd unit (from release dir when fetching, else from the tarball dir)
SERVICE_UNIT_SRC="$(dirname "${LOCAL_BIN}")/telegram-bulk-delivery.service"
if [ ! -f "${SERVICE_UNIT_SRC}" ]; then
    SERVICE_UNIT_SRC="./telegram-bulk-delivery.service"
fi
install -m 0644 "${SERVICE_UNIT_SRC}" /etc/systemd/system/telegram-bulk-delivery.service
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}.service" >/dev/null 2>&1 || true

# 7) Install/upgrade binary — restart only when it changed
NEW_SHA="$(sha256sum "${LOCAL_BIN}" | cut -d' ' -f1)"
OLD_SHA="$(sha256sum "${INSTALL_BIN_DIR}/${BIN_NAME}" 2>/dev/null | cut -d' ' -f1 || true)"
if [ -n "$OLD_SHA" ] && [ "$NEW_SHA" = "$OLD_SHA" ]; then
    echo ">> binary unchanged (${NEW_SHA:0:12}); no restart"
else
    install -m 0755 "${LOCAL_BIN}" "${INSTALL_BIN_DIR}/${BIN_NAME}"
    echo ">> installed ${BIN_NAME} ${VERSION:+v${VERSION}} (${NEW_SHA:0:12})"
    echo ">> restarting ${SERVICE_NAME}.service"
    systemctl restart "${SERVICE_NAME}.service"
fi

# 8) Readiness probe
HEALTHY=""
for _ in $(seq 1 20); do
    if curl -fsS -m 2 "http://127.0.0.1:8080/healthz" >/dev/null 2>&1; then
        HEALTHY=1
        break
    fi
    sleep 0.5
done
systemctl is-active "${SERVICE_NAME}.service" >/dev/null 2>&1 || {
    echo "error: ${SERVICE_NAME}.service failed to start; see: journalctl -u ${SERVICE_NAME} -n 50"
    exit 1
}

echo
echo "== installed =="
echo "  binary:   ${INSTALL_BIN_DIR}/${BIN_NAME}  (${NEW_SHA:0:12})"
echo "  config:   ${CONFIG_DIR}/config.toml"
echo "  env:      ${CONFIG_DIR}/env"
echo "  data:     ${DATA_DIR}"
echo "  key:      ${CONFIG_DIR}/master.key  (BACK THIS UP)"
echo "  service:  ${SERVICE_NAME}.service ($(systemctl is-active "${SERVICE_NAME}.service"))"
if [ -n "$HEALTHY" ]; then
    echo "  health:   http://127.0.0.1:8080/healthz -> ok"
else
    echo "  health:   not yet ready at :8080; check: journalctl -u ${SERVICE_NAME} -n 50"
fi
echo
echo "Put an nginx/caddy TLS proxy in front for public exposure (see README)."