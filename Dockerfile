FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Database volume
VOLUME /app/data
ENV DB_PATH=/app/data/twitterbot.db

# Health check
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import requests; r=requests.get('https://api.telegram.org/bot'+__import__('os').environ.get('BOT_TOKEN','')+'/getMe'); exit(0 if r.ok else 1)" || exit 0

CMD ["python", "bot.py"]
