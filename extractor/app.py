print("Extrator service running...")

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

if __name__ == "__main__":
    print("Extractor service running...", flush=True)
    keep_alive(name="extractor")