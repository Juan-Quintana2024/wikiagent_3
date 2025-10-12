FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Install text-cleaning and HTML parsing dependencies
RUN pip install --no-cache-dir beautifulsoup4 lxml

EXPOSE 8001
CMD ["python", "app.py"]
