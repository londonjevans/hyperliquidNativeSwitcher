FROM python:3.11-slim

# System deps (faster numpy/pandas wheels; add build tools only if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY bot.py /app/bot.py

# Railway sets $PORT; your app already honors it in start_health_server()
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "hyperliquidNativeSwitcher.py"]
