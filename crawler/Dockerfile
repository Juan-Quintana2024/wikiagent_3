FROM python:3.11-slim

# good defaults
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# install Python deps via requirements for better build caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# now copy your code
COPY . .

# no external port needed for the crawler
CMD ["python", "app.py"]
