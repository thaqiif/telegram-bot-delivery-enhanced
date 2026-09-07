# syntax=docker/dockerfile:1

# Runtime-only image for telegram-bulk-delivery.
#
# The binary is NOT compiled here. The release workflow (release.yml) builds
# x86_64 natively and aarch64 by cross-compiling, then copies the right binary
# into a per-platform build context (ctx/<triple>/bin/telegram-bulk-delivery).
# Recompiling inside Docker would push the aarch64 build through QEMU and add
# many minutes for zero benefit — the binary links only glibc, so a slim Debian
# base is sufficient.
#
# The image runs the default operator config (bind 0.0.0.0:8080, SSRF gate on).
# Mount a custom config over /etc/telegram-bulk-delivery/config.toml to change
# limits/ports. Runtime secrets and overrides come from the environment:
#   BULK_MASTER_KEY   (required; base64 of 32 bytes)
#   BULK_API_KEY      (optional; API authentication gate)
#   TELEGRAM_API_BASE (optional; points bots at a Telegram test env / proxy)

# Build arg source: CI-built binary links glibc >= 2.39 (Ubuntu 24.04). Debian
# trixie ships glibc 2.41 (the same as the documented Debian 13 deployment
# target); older bases such as bookworm (2.36) will NOT run the binary.
FROM debian:trixie-slim

ARG VERSION="dev"
ARG SOURCE="https://github.com/thaqiif/telegram-bot-delivery-enhanced"

LABEL org.opencontainers.image.title="telegram-bulk-delivery" \
      org.opencontainers.image.description="Telegram bulk-message delivery service" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.licenses="MIT"

# ca-certificates for reqwest/rustls outbound TLS; curl powers the HEALTHCHECK.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Non-root service user, mirroring the hardened systemd unit (User=tgbulk).
RUN groupadd --gid 10001 tgbulk \
 && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin tgbulk

# Data root. database_path in the operator config is relative, and files/ and
# tmp/ are created beneath the working directory, so the container's CWD IS the
# data directory. Declaring it in the image seeds a fresh named volume with
# tgbulk ownership (host bind-mounts must be chowned to UID 10001 by the caller).
WORKDIR /var/lib/telegram-bulk-delivery
RUN mkdir -p /etc/telegram-bulk-delivery /var/lib/telegram-bulk-delivery \
 && chown -R tgbulk:tgbulk /var/lib/telegram-bulk-delivery

COPY operator.defaults.toml /etc/telegram-bulk-delivery/config.toml
COPY bin/telegram-bulk-delivery /usr/local/bin/telegram-bulk-delivery

EXPOSE 8080

# Reduce capability/privilege surface, matching the systemd unit's hardening:
# run read-only with these in your `docker run`/compose:
#   --read-only \
#   --cap-drop=ALL \
#   --security-opt no-new-privileges:true \
#   -v tgbulk-data:/var/lib/telegram-bulk-delivery
#   -v "$PWD/config.toml":/etc/telegram-bulk-delivery/config.toml:ro
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/healthz >/dev/null || exit 1

USER tgbulk
ENTRYPOINT ["/usr/local/bin/telegram-bulk-delivery", "/etc/telegram-bulk-delivery/config.toml"]