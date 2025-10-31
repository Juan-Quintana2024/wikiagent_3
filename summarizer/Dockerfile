FROM python:3.11-slim

WORKDIR /app
COPY . /app

# 
RUN pip install --no-cache-dir openai requests


EXPOSE 8002

CMD ["python", "app.py"]