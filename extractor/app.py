
#!/usr/bin/env python3
import os, json, glob, time, sqlite3
from bs4 import BeautifulSoup

DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DIR  = os.path.join(DATA_DIR, "raw")     # <- matches crawler
OUT_DIR  = os.path.join(DATA_DIR, "clean")   # <- standard output dir
DB_PATH  = os.getenv("DB_PATH", "/data/wiki.sqlite")
INTERVAL = int(os.getenv("IDLE_INTERVAL", "30"))  # seconds

os.makedirs(OUT_DIR, exist_ok=True)

def db():
    return sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)

def url_and_last_ok(page_id: int):
    conn = db()
    try:
        url = conn.execute("SELECT url FROM pages WHERE id=?", (page_id,)).fetchone()
        url = url[0] if url else None
        ts  = conn.execute(
            "SELECT MAX(fetched_at) FROM fetch_log WHERE page_id=? AND status=200",
            (page_id,)
        ).fetchone()
        return url, (ts[0] if ts else None)
    finally:
        conn.close()

def extract_text(html_bytes: bytes) -> str:
    soup = BeautifulSoup(html_bytes, "lxml")  # faster/more robust than html.parser
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    node = soup.select_one("main, article") or soup.body or soup
    parts = [p.get_text(" ", strip=True) for p in node.find_all("p")]
    return "\n\n".join(p for p in parts if p)

def process_once() -> int:
    wrote = 0
    for html_path in sorted(glob.glob(os.path.join(RAW_DIR, "*.html"))):
        stem = os.path.splitext(os.path.basename(html_path))[0]  # page_id
        out_path = os.path.join(OUT_DIR, f"{stem}.json")
        if os.path.exists(out_path):
            continue  # already processed

        with open(html_path, "rb") as f:
            text = extract_text(f.read())

        try:
            page_id = int(stem)
        except ValueError:
            page_id = None

        url, retrieved_at = (url_and_last_ok(page_id) if page_id is not None else (None, None))
        out = {
            "page_id": page_id,
            "url": url,
            "title": stem,          # placeholder; summarizer can refine
            "content": text,
            "retrieved_at": retrieved_at,
        }
        with open(out_path, "w", encoding="utf-8") as o:
            json.dump(out, o, ensure_ascii=False, indent=2)
        print(f"[extractor] wrote {out_path}", flush=True)
        wrote += 1
    return wrote

if __name__ == "__main__":
    print("Extractor service running...", flush=True)
    while True:
        n = process_once()
        if n == 0:
            time.sleep(INTERVAL)
