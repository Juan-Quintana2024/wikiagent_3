FROM python:3.11-slim

# WORKDIR /app
# COPY . /app

# # Install dependencies for file management, Git automation, and wiki updates
# RUN pip install --no-cache-dir gitpython pyyaml requests

# EXPOSE 8003
# CMD ["python", "app.py"]

# OS deps + Node + TiddlyWiki CLI
RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y nodejs \
 && npm i -g tiddlywiki
 
# Python deps
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir gitpython pyyaml requests

CMD ["python","app.py"]