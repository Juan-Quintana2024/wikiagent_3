#!/usr/bin/env python3                     # Use the system's Python 3 to run this script.

import os, json, time, signal, sys, re     # Stdlib imports: env vars, JSON, sleep, signal handling, sys exit.
from pathlib import Path                   # Path handling with objects instead of plain strings.
from openai import OpenAI                  # OpenAI-compatible client (works with local servers that mimic the API).

# ---- Paths -------------------------------------------------------------------

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))   # Base data dir (overridable via DATA_DIR env var).
CLEAN_DIR = DATA_DIR / "clean"                    # Where cleaned content JSON files live.
SUMMARY_DIR = DATA_DIR / "summarized"             # Where summarized JSON files will be written.

# ---- LLM connection/config ---------------------------------------------------

LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://anjso.ddns.net:13001/api/v1")
LLM_API_KEY  = os.getenv("LLM_API_KEY", "")
MODEL_NAME   = os.getenv("LLM_MODEL", "anjso")
# LLM_BASE_URL = os.getenv("LLM_BASE_URL", "http://host.docker.internal:1234/v1")  # OpenAI-compatible HTTP endpoint.
# LLM_API_KEY  = os.getenv("LLM_API_KEY", "local")                                  # Dummy/real key depending on server.
# MODEL_NAME   = os.getenv("LLM_MODEL", "meta-llama-3.1-8b-instruct")               # Model to ask for.
INTERVAL     = int(os.getenv("IDLE_INTERVAL", "60"))  # Seconds to sleep when there’s no new work.

client = OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY)  # Create the API client bound to your local server.

print(f"Connecting to LLM at {LLM_BASE_URL}", flush=True)
print(f"Using model: {MODEL_NAME}", flush=True)


# ---- Graceful shutdown -------------------------------------------------------

def _graceful_exit(signum, frame):
    print("Summarizer shutting down...", flush=True)  # Log that we’re stopping.
    sys.exit(0)                                       # Exit cleanly (lets Docker/system stop without errors).

signal.signal(signal.SIGINT, _graceful_exit)          # Handle Ctrl+C.
signal.signal(signal.SIGTERM, _graceful_exit)         # Handle `docker stop` / systemd termination.

# ---- Utility: safe slug for filenames ----------------------------------------
# Added this to convert generated titles into safe filenames.

def slugify(text):
    """Convert title text into a safe lowercase filename slug."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)  # Replace non-alphanumerics with dashes
    text = re.sub(r"-+", "-", text).strip("-")  # Collapse repeats
    text = text[:80]  # Limits filename length to avoid OS path issues
    return text or "untitled"

# ---- Token estimation and chunking -----------------------------
# Added to handle inputs that exceed the 4096-token context limit.

def approx_tokens(s: str) -> int:
    """Roughly estimate token count (≈4 chars per token)."""
    return max(1, len(s) // 4)

MAX_TOKENS = 4096                # Model hard limit
SAFE_INPUT_TOKENS = 3000         # Input budget per request
OVERLAP_TOKENS = 150             # Small overlap between chunks

def chunk_text(text, max_tokens=SAFE_INPUT_TOKENS, overlap=OVERLAP_TOKENS):
    """Split long text into manageable overlapping chunks."""
    if approx_tokens(text) <= max_tokens:
        return [text]

    chunks = []
    step_chars = max_tokens * 4
    overlap_chars = overlap * 4

    start = 0
    while start < len(text):
        end = min(len(text), start + step_chars)
        chunk = text[start:end]
        chunks.append(chunk.strip())
        start = end - overlap_chars  # step back slightly to preserve context

    return [c for c in chunks if c]

# ---- Core LLM call -----------------------------------------------------------
# Adaptive Summarizer

def summarize_text(text, max_chunk_size=4000, chunk_threshold=8000):
    """
    Summarize text using the local LLM.
    If the text is larger than chunk_threshold characters, automatically chunk it.
    """
    try:
        if len(text) > chunk_threshold:
            print(f"[summarizer] Large file detected ({len(text)} chars) → using chunking...", flush=True)
            chunks = [text[i:i + max_chunk_size] for i in range(0, len(text), max_chunk_size)]
            partial_summaries = []

            for i, chunk in enumerate(chunks):
                print(f"[summarizer] Summarizing chunk {i+1}/{len(chunks)} ({len(chunk)} chars)...", flush=True)
                resp = client.chat.completions.create(
                    model=MODEL_NAME,
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "You are a summarizer that writes concise, factual summaries "
                                "in under 150 words suitable for a wiki entry."
                            ),
                        },
                        {"role": "user", "content": chunk},
                    ],
                )
                reply = resp.choices[0].message.content.strip()
                partial_summaries.append(reply)

            combined_text = "\n".join(partial_summaries)
            print("[summarizer] Combining partial summaries...", flush=True)

            resp = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Condense the following section summaries into one concise wiki-style paragraph "
                            "under 150 words."
                        ),
                    },
                    {"role": "user", "content": combined_text},
                ],
            )
            return resp.choices[0].message.content.strip()

        # Normal mode for small texts
        else:
            resp = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a summarizer that writes concise, factual summaries "
                            "in under 150 words suitable for a wiki entry."
                        ),
                    },
                    {"role": "user", "content": text},
                ],
            )
            if hasattr(resp, "choices"):
                return resp.choices[0].message.content.strip()
            elif isinstance(resp, dict) and "choices" in resp:
                return resp["choices"][0]["message"]["content"].strip()
            elif isinstance(resp, str):
                return resp.strip()
            else:
                return json.dumps(resp, ensure_ascii=False)[:200]

    except Exception as e:
        print(f"[ERROR] LLM summarization failed: {e}", flush=True)
        return None
'''
# Simple Summarizer
def summarize_text(text):
    """Send text to the local LLM and return a short summary."""
    try:
        resp = client.chat.completions.create(        # OpenAI Chat Completions API call.
            model=MODEL_NAME,                         # Ask for the configured model.
            messages=[                                # 2-message chat: system + user.
                {
                    "role": "system",
                    "content": (
                        "You are a summarizer that writes concise, factual summaries "
                        "in under 150 words suitable for a wiki entry."
                    ),
                },
                {"role": "user", "content": text},    # The article/content to summarize.
            ],
        )

        # --- Handle multiple possible response formats ---
        if hasattr(resp, "choices"):  # Normal OpenAI object
            return resp.choices[0].message.content.strip()
        elif isinstance(resp, dict) and "choices" in resp:  # JSON dict style
            return resp["choices"][0]["message"]["content"].strip()
        elif isinstance(resp, str):  # Raw string returned directly
            return resp.strip()
        else:  # Fallback: return partial JSON if something unexpected
            return json.dumps(resp, ensure_ascii=False)[:500]

    except Exception as e:
        print(f"[ERROR] LLM summarization failed: {e}", flush=True)  # Log failures but don’t crash the service.
        return None
'''
    
# Ask the LLM to produce a short wiki-style title.

def generate_title(text):
    """Ask the LLM for a short, relevant title (max ~10 words)."""
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a title generator. Write a short, factual title "
                        "for a wiki page based on the provided text, ideally under 10 words. "
                        "Do not include quotes or punctuation at the end."
                    ),
                },
                {"role": "user", "content": text},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as e:
        print(f"[ERROR] LLM title generation failed: {e}", flush=True)
        return None    

# ---- One scan pass over /data/clean -----------------------------------------

def process_once():
    """Scan /data/clean for new .json files and summarize them."""
    wrote = 0
    for json_path in sorted(CLEAN_DIR.glob("*.json")):
        out_path = SUMMARY_DIR / json_path.name
        if out_path.exists():
            continue  # already summarized

        try:
            # --- Load and validate JSON ---
            raw_text = json_path.read_text(encoding="utf-8")
            if not raw_text.strip():
                print(f"[WARN] {json_path.name} is empty, skipping.", flush=True)
                continue

            try:
                data = json.loads(raw_text)
            except json.JSONDecodeError:
                print(f"[WARN] {json_path.name} is not valid JSON, skipping.", flush=True)
                continue

            content = data.get("content", "")
            if not isinstance(content, str) or not content.strip():
                print(f"[WARN] {json_path.name} has no usable content, skipping.", flush=True)
                continue

            # --- Summarize the content ---
            print(f"[summarizer] Summarizing {json_path.name}...", flush=True)
            summary = summarize_text(content)

            if not summary:
                print(f"[WARN] No summary returned for {json_path.name}, skipping.", flush=True)
                continue

            # --- Generate title ---
            title = generate_title(summary or content)
            if not title:
                title = str(data.get("page_id", "Untitled"))

            # --- Prepare final JSON ---
            data["summary"] = summary
            data["title"] = title
            slug = slugify(title)

            # --- Determine output filename ---
            page_id = str(data.get("page_id", "")).strip()
            out_path = SUMMARY_DIR / (f"{page_id}.json" if page_id else json_path.name)

            # --- Write summarized file ---
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            print(f"[summarizer] ✅ Saved summary as {out_path.name}", flush=True)
            wrote += 1

        except Exception as e:
            print(f"[WARN] ❌ Failed to process {json_path.name}: {e}", flush=True)

    return wrote  # How many files were successfully summarized this pass.


# ---- Service entrypoint / main loop -----------------------------------------

if __name__ == "__main__":
    print(f"Summarizer service running... (model={MODEL_NAME})", flush=True)
    print(f"Connecting to LLM at {LLM_BASE_URL}", flush=True)

    # Quick test connection (best-effort; some servers may not implement .models.list()).
    try:
        models = client.models.list()                                  # Probe server; returns a list of models.
        print(f"LLM reachable, {len(models.data)} models available.", flush=True)
    except Exception as e:
        print(f"[WARN] Could not verify LLM: {e}", flush=True)         # Don’t crash if the endpoint lacks this route.

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)                     # Make sure output directory exists.

    while True:                                                        # Daemon loop: do work, then idle if none.
        n = process_once()                                             # Do a single scan/summarize pass.
        if n == 0:                                                     # If nothing new was written…
            time.sleep(INTERVAL)                                       # …sleep to avoid busy-waiting.