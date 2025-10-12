FROM python:3.11-slim

WORKDIR /app
COPY . /app

# Install LLM-related or NLP dependencies (you can replace later with ollama client libs)
RUN pip install --no-cache-dir transformers torch sentencepiece

EXPOSE 8002
CMD ["python", "app.py"]
