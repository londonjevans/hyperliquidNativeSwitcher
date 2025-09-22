FROM python:3.11-slim

# System deps for SSL + basic tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source (respects .dockerignore)
COPY . .

# Railway exposes $PORT; your app binds to it already
ENV PYTHONUNBUFFERED=1

# IMPORTANT: run the correct filename here
# If your main file is "hyperliquidNativeSwitcher.py", keep it:
CMD ["python", "-u", "hyperliquidNativeSwitcher.py"]

