FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Install dependencies for file management, Git automation, and wiki updates
RUN pip install --no-cache-dir gitpython pyyaml requests

EXPOSE 8003
CMD ["python", "app.py"]
