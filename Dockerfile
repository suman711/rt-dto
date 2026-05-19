# Use Python 3.11 slim — matches our development environment exactly
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed by psycopg (PostgreSQL client libs)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (Docker layer caching)
# This layer only rebuilds when requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY worker_entrypoint.py .
COPY .env .

# Copy environment file
COPY .env .

# Expose API port
EXPOSE 8000

# Default command — runs the FastAPI app
# Workers are launched inside the app via lifespan startup
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]