print("Summarizer service running...")

import os, time, sys, signal

def _graceful_exit(signum, frame):
    print("Shutting down...", flush=True)
    sys.exit(0)

signal.signal(signal.SIGINT, _graceful_exit)   # Ctrl+C / docker stop
signal.signal(signal.SIGTERM, _graceful_exit)  # docker stop

def keep_alive(name="service"):
    interval = int(os.getenv("IDLE_INTERVAL", "60"))  # seconds
    print(f"{name} idle loop started (interval={interval}s)", flush=True)
    while True:
        time.sleep(interval)

def _check_llm():
    try:
        from openai import OpenAI
        base = os.getenv("LLM_BASE_URL", "http://host.docker.internal:1234/v1")
        key  = os.getenv("LLM_API_KEY", "local")
        OpenAI(base_url=base, api_key=key).models.list()
        print(f"LLM reachable at {base}", flush=True)
    except Exception as e:
        print(f"[WARN] LLM check failed: {e}", flush=True)

if __name__ == "__main__":
    print("Summarizer service running...", flush=True)
    _check_llm()
    keep_alive(name="summarizer")