FROM python:3.11-slim

# Install Firefox ESR + dependencies
RUN apt-get update && apt-get install -y \
    firefox-esr \
    wget \
    ca-certificates \
    libgtk-3-0 \
    libdbus-glib-1-2 \
    libx11-xcb1 \
    && rm -rf /var/lib/apt/lists/*

# Install geckodriver
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v0.35.0/geckodriver-v0.35.0-linux64.tar.gz \
    && tar -xzf geckodriver-v0.35.0-linux64.tar.gz \
    && mv geckodriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver \
    && rm geckodriver-v0.35.0-linux64.tar.gz

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create downloads folder
RUN mkdir -p /root/Downloads/Cost_scraped

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "dashboard.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
