FROM python:3.11-slim

# ── System deps: Firefox ESR + curl (for healthcheck) ────────────────────────
RUN apt-get update && apt-get install -y \
    firefox-esr \
    wget \
    curl \
    ca-certificates \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libx11-xcb1 \
    && rm -rf /var/lib/apt/lists/*

# ── Geckodriver ───────────────────────────────────────────────────────────────
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz \
    && tar -xzf geckodriver-v0.35.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver \
    && rm geckodriver-v0.35.0-linux64.tar.gz

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent folders (overridden by Docker volumes in production)
RUN mkdir -p /app/data /root/Downloads/Cost_scraped

# Default: FastAPI control-panel API
# Override in docker-compose.yml for the dashboard service
EXPOSE 8000 8501

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
