#!/bin/bash
# ============================================================
#  Afro ERP — One-command VPS deployment script
#  Usage:  bash deploy.sh
#  Tested: Ubuntu 20.04 / 22.04 / 24.04
# ============================================================
set -e   # stop on any error

REPO_URL="https://github.com/motaha-2020/afro-cost-dashboard.git"
APP_DIR="/opt/afro-erp"
COMPOSE_FILE="$APP_DIR/docker-compose.yml"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log()  { echo -e "${GREEN}[+]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[x]${NC} $1"; exit 1; }

# ── Must run as root ──────────────────────────────────────────────────────────
[ "$(id -u)" -eq 0 ] || err "Run as root:  sudo bash deploy.sh"

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║   Afro ERP — VPS Deployment              ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# ── 1. System update ──────────────────────────────────────────────────────────
log "Updating system packages…"
apt-get update -qq && apt-get upgrade -y -qq

# ── 2. Install Docker ─────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    log "Installing Docker…"
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    log "Docker installed: $(docker --version)"
else
    log "Docker already installed: $(docker --version)"
fi

# docker compose v2 (plugin)
if ! docker compose version &>/dev/null; then
    log "Installing Docker Compose plugin…"
    apt-get install -y -qq docker-compose-plugin
fi
log "Docker Compose: $(docker compose version)"

# ── 3. Install Git ────────────────────────────────────────────────────────────
if ! command -v git &>/dev/null; then
    log "Installing git…"
    apt-get install -y -qq git
fi

# ── 4. Clone or update repo ───────────────────────────────────────────────────
if [ -d "$APP_DIR/.git" ]; then
    log "Repo exists — pulling latest code…"
    git -C "$APP_DIR" pull origin main
else
    log "Cloning repo to $APP_DIR…"
    git clone "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

# ── 5. Create .env if missing ─────────────────────────────────────────────────
if [ ! -f "$APP_DIR/.env" ]; then
    warn ".env file not found — creating from .env.example"
    cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    echo ""
    echo "  ┌─────────────────────────────────────────────┐"
    echo "  │  Enter your ERP credentials:                │"
    echo "  └─────────────────────────────────────────────┘"
    read -rp "  ERP Username  [motaha]: " erp_user
    erp_user=${erp_user:-motaha}
    read -rsp "  ERP Password: " erp_pass; echo
    read -rsp "  Access Code : " erp_access; echo
    sed -i "s/^AFRO_USER=.*/AFRO_USER=$erp_user/"   "$APP_DIR/.env"
    sed -i "s/^AFRO_PASS=.*/AFRO_PASS=$erp_pass/"   "$APP_DIR/.env"
    sed -i "s/^AFRO_ACCESS=.*/AFRO_ACCESS=$erp_access/" "$APP_DIR/.env"
    log ".env saved."
else
    log ".env already exists — skipping credential prompt."
fi

# ── 6. Configure UFW firewall ─────────────────────────────────────────────────
if command -v ufw &>/dev/null; then
    log "Opening firewall ports 8000 and 8501…"
    ufw allow 8000/tcp  comment "Afro ERP Control Panel" || true
    ufw allow 8501/tcp  comment "Afro ERP Dashboard"     || true
    ufw --force enable  || true
fi

# ── 7. Build & start containers ───────────────────────────────────────────────
log "Building Docker image (first time ~5-10 min)…"
docker compose -f "$COMPOSE_FILE" build --no-cache

log "Starting services…"
docker compose -f "$COMPOSE_FILE" up -d

# ── 8. Wait for API health check ──────────────────────────────────────────────
log "Waiting for API to be ready…"
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/health >/dev/null 2>&1; then
        log "API is up!"
        break
    fi
    sleep 3
done

# ── 9. Auto-restart on reboot (systemd service) ───────────────────────────────
SERVICE_FILE="/etc/systemd/system/afro-erp.service"
if [ ! -f "$SERVICE_FILE" ]; then
    log "Creating systemd service for auto-start on reboot…"
    cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Afro ERP Dashboard
Requires=docker.service
After=docker.service network.target

[Service]
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable afro-erp.service
    log "Systemd service registered — app will auto-start after reboot."
fi

# ── 10. Get server IP ─────────────────────────────────────────────────────────
SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║   Deployment complete!                                       ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║                                                              ║"
echo -e "  ║   Control Panel  →  http://${SERVER_IP}:8000           ║"
echo -e "  ║   Dashboard      →  http://${SERVER_IP}:8501           ║"
echo "  ║                                                              ║"
echo "  ║   docker compose logs -f api        (API logs)              ║"
echo "  ║   docker compose logs -f dashboard  (Dashboard logs)        ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""
