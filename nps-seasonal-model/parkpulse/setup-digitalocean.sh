#!/usr/bin/env bash
#
# ParkPulse collector — DigitalOcean droplet setup
#
# Run as root on a fresh Ubuntu 24.04 droplet:
#   curl -sSL <raw-gist-url> | bash
#
# Or copy this script to the droplet and run:
#   chmod +x setup-digitalocean.sh && sudo ./setup-digitalocean.sh
#
# After running, edit /opt/parkpulse/.env with your RIDB_API_KEY,
# then: sudo systemctl start parkpulse-collector
#
set -euo pipefail

echo "==> Installing system packages"
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip git

echo "==> Creating parkpulse user"
id -u parkpulse &>/dev/null || useradd --system --create-home --home-dir /opt/parkpulse --shell /usr/sbin/nologin parkpulse

echo "==> Cloning repository"
if [ ! -d /opt/parkpulse/nps-seasonal-model ]; then
    git clone https://github.com/jhcwalsh/NationalParks.git /opt/parkpulse/repo
    ln -sf /opt/parkpulse/repo/nps-seasonal-model /opt/parkpulse/nps-seasonal-model
else
    echo "    Repository already exists, pulling latest"
    cd /opt/parkpulse/repo && git pull origin master
fi

echo "==> Creating Python venv and installing dependencies"
python3 -m venv /opt/parkpulse/venv
/opt/parkpulse/venv/bin/pip install --quiet --upgrade pip
/opt/parkpulse/venv/bin/pip install --quiet -r /opt/parkpulse/nps-seasonal-model/requirements.txt

echo "==> Creating data directory"
mkdir -p /opt/parkpulse/data

echo "==> Writing env file template"
if [ ! -f /opt/parkpulse/.env ]; then
    cat > /opt/parkpulse/.env <<'ENVEOF'
# ParkPulse collector environment
# The collector only needs the RIDB key — get one free at https://ridb.recreation.gov
RIDB_API_KEY=your_key_here
ENVEOF
    echo "    *** Edit /opt/parkpulse/.env with your real RIDB_API_KEY ***"
else
    echo "    .env already exists, skipping"
fi

echo "==> Setting ownership"
chown -R parkpulse:parkpulse /opt/parkpulse

echo "==> Installing systemd service"
cp /opt/parkpulse/nps-seasonal-model/parkpulse/parkpulse-collector.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable parkpulse-collector

echo ""
echo "============================================"
echo "  Setup complete!"
echo "============================================"
echo ""
echo "  Next steps:"
echo "    1. Edit your API key:"
echo "       sudo nano /opt/parkpulse/.env"
echo ""
echo "    2. Start the collector:"
echo "       sudo systemctl start parkpulse-collector"
echo ""
echo "    3. Check the logs:"
echo "       journalctl -u parkpulse-collector -f"
echo ""
echo "    4. Verify it's running:"
echo "       systemctl status parkpulse-collector"
echo ""
echo "  The DuckDB database lives at:"
echo "    /opt/parkpulse/data/parkpulse.duckdb"
echo ""
echo "  To query it (while the collector runs):"
echo "    sudo -u parkpulse /opt/parkpulse/venv/bin/python -c \\"
echo "      \"import duckdb; print(duckdb.connect('/opt/parkpulse/data/parkpulse.duckdb', read_only=True).execute('SELECT * FROM poll_log ORDER BY poll_id DESC LIMIT 5').fetchdf())\""
echo ""
