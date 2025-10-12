# Base image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files into the container
COPY . /app

# Install dependencies (add your crawler requirements here)
RUN pip install --no-cache-dir requests beautifulsoup4 playwright

# Expose nothing externally (internal network use only)
EXPOSE 8000

# Default command
CMD ["python", "app.py"]
