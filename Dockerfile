# Use the official Python image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the requirements file first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project into the container
COPY . .

# Expose the port (optional, if the pipeline has an API or server)
# EXPOSE 8000

# Define environment variables (useful for settings)
ENV POLL_INTERVAL=60
ENV USE_BATCH=false
ENV BATCH_SIZE=100

# Run the pipeline (main entry point)
CMD ["python", "scripts/main.py"]
