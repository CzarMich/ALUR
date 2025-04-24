# --------------------------------------
# Alur Dockerfile
# --------------------------------------

# Use official Python slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install OS dependencies for psycopg2, cryptography, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    libffi-dev \
    libssl-dev \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY . .

# Ensure required folders exist
RUN mkdir -p \
    logs \
    data \
    conf/environment/cert \
    conf/environment/key \
    conf/resources

# Auto-generate .env if missing
RUN python generate_env.py

# Environment Variables (can be overridden at runtime)
ENV POLL_INTERVAL=1800
ENV PYTHONUNBUFFERED=1

# Expose ports if needed
# EXPOSE 8080

# Optional: run as non-root user
# RUN useradd -m appuser && chown -R appuser /app
# USER appuser

# Run the application
CMD ["python", "application/main.py"]
