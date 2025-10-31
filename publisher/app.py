# print("Publisher service running...")

# import os, time, sys, signal

# def _graceful_exit(signum, frame):
#     print("Shutting down...", flush=True)
#     sys.exit(0)

# signal.signal(signal.SIGINT, _graceful_exit)   # Ctrl+C / docker stop
# signal.signal(signal.SIGTERM, _graceful_exit)  # docker stop

# def keep_alive(name="service"):
#     interval = int(os.getenv("IDLE_INTERVAL", "60"))  # seconds
#     print(f"{name} idle loop started (interval={interval}s)", flush=True)
#     while True:
#         time.sleep(interval)

# if __name__ == "__main__":
#     print("Publisher service running...", flush=True)
#     keep_alive(name="publisher")


# Adding this for the publisher section

#!/usr/bin/env python3
import os, json, subprocess, hashlib, re
from pathlib import Path
from datetime import datetime

DATA_DIR     = Path(os.getenv("DATA_DIR", "/data"))
SUMMARY_DIR  = Path(os.getenv("SUMMARY_DIR", str(DATA_DIR / "summarized")))
SITE_DIR     = Path(os.getenv("SITE_DIR", "/site"))
WIKI_WORKDIR = Path(os.getenv("WIKI_WORKDIR", "/tmp/wiki"))

_slug_re = re.compile(r"[^a-z0-9-_]")
def slugify(s: str) -> str:
    s = (s or "untitled").lower().strip().replace(" ", "-")
    s = _slug_re.sub("-", s)
    return re.sub(r"-{2,}", "-", s)[:80]

def ensure_tw_project():
    (WIKI_WORKDIR / "tiddlers").mkdir(parents=True, exist_ok=True)
    info = {
        "description": "Auto-generated wiki",
        "plugins": ["tiddlywiki/tiddlyweb", "tiddlywiki/filesystem"],
        "build": {
            "index": ["--rendertiddler","$:/core/save/all","output/index.html","text/plain"]
        }
    }
    (WIKI_WORKDIR / "tiddlywiki.info").write_text(json.dumps(info), encoding="utf-8")

def create_tiddlers() -> int:
    tiddlers_dir = WIKI_WORKDIR / "tiddlers"
    tiddlers_dir.mkdir(parents=True, exist_ok=True)
    count = 0
    for json_path in Path(SUMMARY_DIR).glob("*.json"):
        try:
            data = json.loads(json_path.read_text(encoding="utf-8-sig"))
            title   = data.get("title") or json_path.stem
            body    = data.get("summary") or data.get("text") or "No summary available."
            tags    = data.get("tags") or ["summary"]
            created = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            sid     = hashlib.sha1(title.encode("utf-8")).hexdigest()[:8]
            fname   = f"{slugify(title)}-{sid}.tid"
            tagstr  = " ".join(tags if isinstance(tags, list) else [str(tags)])
            tid = (
                f"title: {title}\n"
                f"tags: {tagstr}\n"
                f"type: text/vnd.tiddlywiki\n"
                f"created: {created}\n"
                f"modified: {created}\n\n"
                f"{body}\n"
            )
            (tiddlers_dir / fname).write_text(tid, encoding="utf-8")
            count += 1
        except Exception as e:
            print(f"[WARN] failed {json_path.name}: {e}", flush=True)
    print(f"[publisher] Created {count} tiddlers from {SUMMARY_DIR}")
    return count

def inject_theme_tiddlers():
    """Add theme and style overrides before building the wiki."""
    tiddlers_dir = WIKI_WORKDIR / "tiddlers"
    tiddlers_dir.mkdir(parents=True, exist_ok=True)

    # Theme selection
    theme_tid = tiddlers_dir / "$__theme.tid"
    theme_tid.write_text(
        "title: $:/theme\n"
        "type: text/plain\n\n"
        "$:/themes/tiddlywiki/vanilla\n",
        encoding="utf-8"
    )

    # Custom stylesheet
    style_tid = tiddlers_dir / "$__themes__custom__anjso__style.tid"
    style_css = """title: $:/themes/custom/anjso/style.css
tags: [[$:/tags/Stylesheet]]
type: text/css

/* Base Page Colors */
body {
  font-family: "Inter", sans-serif;
  background-color: #1a1a1a;
  color: #f0f0f0;
  max-width: 900px;
  margin: 0 auto;
  line-height: 1.6;
}

/* Tiddler Frames */
.tc-tiddler-frame {
  background: #666666;
  border-radius: 8px;
  padding: 1.5em;
  margin-top: 1em;
  box-shadow: 0 0 4px rgba(0,0,0,0.4);
}

/* Links */
a {
  color: #5ec2e8;
  text-decoration: none;
}
a:hover {
  text-decoration: underline;
}

/* Headings */
h1, h2, h3 {
  color: #e0e0e0;
}
.tc-subtitle {
  color: #aaaaaa;
}

"""
    
    style_tid.write_text(style_css, encoding="utf-8")
    print("[publisher] Injected theme and stylesheet tiddlers", flush=True)
    # Custom site title
    site_title_tid = tiddlers_dir / "$__SiteTitle.tid"
    site_title_tid.write_text(
        "title: $:/SiteTitle\n"
        "type: text/vnd.tiddlywiki\n\n"
        "Nanjing Knowledge Hub Wiki",
        encoding="utf-8"
    )

def build_wiki():
    print("[publisher] Building wiki…", flush=True)
    ensure_tw_project()
    outdir = WIKI_WORKDIR / "output"
    outdir.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "tiddlywiki", str(WIKI_WORKDIR),
        "--output", str(outdir),
        "--rendertiddler", "$:/core/save/all", "index.html", "text/plain"
    ], check=True)
    built_html = outdir / "index.html"
    SITE_DIR.mkdir(parents=True, exist_ok=True)
    if built_html.exists():
        (SITE_DIR / "index.html").write_text(built_html.read_text(encoding="utf-8"))
        print(f"[publisher] Wrote {SITE_DIR / 'index.html'}", flush=True)
    else:
        print("[publisher] ERROR: TiddlyWiki did not produce index.html", flush=True)

def main():
    print(f"[publisher] SUMMARY_DIR={SUMMARY_DIR} SITE_DIR={SITE_DIR}", flush=True)
    created = create_tiddlers()
    if created == 0:
        print("[publisher] No summaries found; nothing to publish.", flush=True)
        return
    inject_theme_tiddlers()  # add custom theme before building wiki.
    build_wiki()

if __name__ == "__main__":
    main()