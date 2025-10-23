#!/usr/bin/env python3
import os, re, time, queue, threading, urllib.parse, urllib.robotparser
from datetime import datetime
import requests, yaml
from bs4 import BeautifulSoup

DATA_DIR = "/data"
RAW_DIR = os.path.join(DATA_DIR, "raw")
CFG_PATH = os.environ.get("CRAWL_CFG", "/config/whitelist.yml")
UA = "ANJSO-WikiCrawler/1.0 (+https://anjso.org/wiki)"
os.makedirs(RAW_DIR, exist_ok=True)

def canon_url(u):
    u = urllib.parse.urldefrag(u)[0]
    p = urllib.parse.urlsplit(u)
    if p.query and "action=edit" in p.query:
        return None
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, p.query, ""))

def compile_patterns(pats): return [re.compile(p) for p in (pats or [])]

def allowed(u, inc, exc):
    if inc and not any(r.search(u) for r in inc): return False
    if exc and any(r.search(u) for r in exc): return False
    return True

_rp_cache = {}
def robots_ok(url, agent=UA):
    host = urllib.parse.urlsplit(url).netloc
    rp = _rp_cache.get(host)
    if not rp:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(f"https://{host}/robots.txt")
        try: rp.read()
        except Exception: return False
        _rp_cache[host] = rp
    return rp.can_fetch(agent, url)

def extract_links(base_url, html):
    out = []
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = a.get("href"); 
        if not href: continue
        absu = urllib.parse.urljoin(base_url, href)
        absu = canon_url(absu)
        if absu: out.append(absu)
    return out

def crawl():
    cfg = yaml.safe_load(open(CFG_PATH))
    inc = compile_patterns(cfg.get("include_patterns"))
    exc = compile_patterns(cfg.get("exclude_patterns"))
    max_pages = int(cfg.get("limits", {}).get("max_pages", 900))
    max_depth = int(cfg.get("limits", {}).get("max_depth", 2))
    rps = float(cfg.get("rate_limit", {}).get("per_host_rps", 2))
    workers = int(cfg.get("rate_limit", {}).get("max_parallel", 12))
    respect_robots = bool(cfg.get("respect_robots", True))
    print(f"[cfg] max_pages={max_pages} depth={max_depth} workers={workers} rps/host={rps} robots={respect_robots}", flush=True)

    frontier = queue.Queue()
    seen = set()

    for s in cfg.get("seeds", []):
        su = canon_url(s)
        if su and allowed(su, inc, exc):
            frontier.put((su, 0))

    fetched = 0
    last_hit = {}
    lock = threading.Lock()

    def rate_limit(host):
        now = time.monotonic()
        nxt = last_hit.get(host, 0) + 1.0/max(rps, 0.1)
        slp = max(0.0, nxt - now)
        if slp: time.sleep(slp)
        last_hit[host] = time.monotonic()

    def worker():
        nonlocal fetched
        sess = requests.Session()
        headers = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml"}
        while True:
            try: url, depth = frontier.get(timeout=1)
            except queue.Empty: return
            if url in seen: frontier.task_done(); continue
            seen.add(url)

            if respect_robots and not robots_ok(url):
                frontier.task_done(); continue

            host = urllib.parse.urlsplit(url).netloc
            rate_limit(host)
            try:
                resp = sess.get(url, timeout=20, headers=headers)
                if resp.status_code != 200 or "text/html" not in resp.headers.get("Content-Type",""):
                    frontier.task_done(); continue
                html = resp.content

                # simple id = current fetched+1 (good enough for raw dump)
                with lock:
                    fetched += 1
                    pid = fetched
                with open(os.path.join(RAW_DIR, f"{pid}.html"), "wb") as f:
                    f.write(html)
                if pid % 25 == 0:
                    print(f"[prog] fetched={pid} frontier≈{frontier.qsize()} time={datetime.utcnow().isoformat()}Z", flush=True)

                if depth + 1 <= max_depth:
                    for to in extract_links(url, html):
                        if allowed(to, inc, exc) and to not in seen:
                            frontier.put((to, depth + 1))

                if fetched >= max_pages:
                    # drain queue and stop workers
                    while not frontier.empty():
                        try: frontier.get_nowait(); frontier.task_done()
                        except queue.Empty: break
                    frontier.task_done()
                    return
            except Exception as e:
                print("[warn]", e, flush=True)
            finally:
                frontier.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for t in threads: t.start()
    for t in threads: t.join()

    print(f"[done] fetched={fetched}", flush=True)

# --- keep the container up after run so you can inspect files/logs ---
import sys, signal
def _graceful(signum, frame): print("Crawler shutting down...", flush=True); sys.exit(0)
signal.signal(signal.SIGINT, _graceful)
signal.signal(signal.SIGTERM, _graceful)

if __name__ == "__main__":
    print("Crawler service running...", flush=True)
    crawl()
    while True: time.sleep(60)
