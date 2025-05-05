# --------------------------------------
# ALUR Dockerfile
# --------------------------------------

    FROM python:3.13-alpine

    # Set working directory inside the container
    WORKDIR /app
    
    # Install build and runtime dependencies
    RUN apk add --no-cache \
        build-base \
        libffi-dev \
        openssl-dev \
        postgresql-dev \
        musl-dev
    
    # Install Python dependencies
    COPY requirements.txt .
    RUN pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt
    
    # Copy application code and resources
    COPY application/ ./application/
    
    # Pre-compile Python files to .pyc and strip all .py source
    RUN python -m compileall -b ./application \
     && find ./application -name "*.py" -delete
    
    # Create data/log folders and restrict permissions
    RUN mkdir -p /app/data /app/logs \
     && chmod -R a-w ./application
    
    # Use a non-root user for container runtime
    RUN adduser -D aluruser
    USER aluruser
    
    # Define environment variables (can be overridden at runtime)
    ENV POLL_INTERVAL=1800
    ENV PYTHONUNBUFFERED=1
    
    # Default command
    CMD ["python", "application/main.py"]
    