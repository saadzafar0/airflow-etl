"""Capture genuine browser screenshots of source excerpts and saved outputs."""

import argparse
import html
from pathlib import Path
import shutil
import subprocess

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import PythonLexer, YamlLexer

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "reports" / "evidence"


def capture(firefox, profile, name, title, content, height):
    page = EVIDENCE / f"{name}.html"
    formatter = HtmlFormatter(style="friendly")
    page.write_text(f"""<!doctype html><meta charset="utf-8"><style>
    body {{margin:0;background:#f3f6fa;color:#10253b;font-family:Arial,sans-serif}}
    header {{padding:25px 36px;background:#10253b;color:white;font-size:25px}}
    main {{margin:25px;padding:24px;background:white;border:1px solid #d5dfe9;border-radius:8px}}
    pre {{margin:0;font-family:'DejaVu Sans Mono',monospace;font-size:22px;line-height:1.42;white-space:pre-wrap;overflow-wrap:anywhere}}
    {formatter.get_style_defs('.highlight')}
    .highlight {{background:white}}
    </style><header>{html.escape(title)}</header><main>{content}</main>""", encoding="utf-8")
    destination = EVIDENCE / f"{name}.png"
    subprocess.run([
        firefox, "--headless", "--no-remote", "--profile", str(profile),
        "--window-size", f"1440,{height}", "--screenshot", str(destination), page.as_uri(),
    ], check=True, timeout=60, stdout=subprocess.DEVNULL)
    print(destination.relative_to(ROOT))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--firefox", default=shutil.which("firefox"))
    args = parser.parse_args()
    if not args.firefox:
        parser.error("Provide --firefox /path/to/firefox")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    profile = Path("/tmp/airflow-etl-report-firefox")
    profile.mkdir(exist_ok=True)
    (profile / "user.js").write_text('user_pref("network.proxy.type", 0);\n')
    specs = [
        ("code_dag_tasks", "dags/currency_exchange_etl.py", 17, 48, PythonLexer()),
        ("code_dag_schedule", "dags/currency_exchange_etl.py", 51, 79, PythonLexer()),
        ("code_extract", "include/currency_etl.py", 1, 44, PythonLexer()),
        ("code_transform", "include/currency_etl.py", 45, 82, PythonLexer()),
        ("code_load", "include/currency_etl.py", 85, 108, PythonLexer()),
        ("code_docker", "docker-compose.override.yml", 1, 75, YamlLexer()),
    ]
    for name, source, first, last, lexer in specs:
        lines = (ROOT / source).read_text().splitlines()[first - 1:last]
        numbered = "\n".join(f"{number:3}  {line}" for number, line in enumerate(lines, first))
        content = highlight(numbered, lexer, HtmlFormatter(style="friendly"))
        capture(args.firefox, profile, name, f"Source excerpt | {source} | lines {first}-{first+len(lines)-1}", content, 170 + 32 * len(lines))
    for name, filename, title in [
        ("output_api", "live-api.json", "Captured live Frankfurter API response"),
        ("output_transform", "transformed-records.json", "Actual transformed records from the live API"),
        ("output_tests", "unit-tests.txt", "Actual pytest output"),
        ("output_runtime", "runtime-summary.txt", "Actual Astro / Airflow execution output"),
        ("output_database", "mongodb-verification.json", "Actual persisted MongoDB records and unique index"),
        ("output_docker", "docker-status.txt", "Actual local Docker services, network and volumes"),
        ("output_git", "git-history.txt", "Actual incremental Git commit history"),
    ]:
        path = EVIDENCE / filename
        if path.exists():
            text = path.read_text()
            capture(args.firefox, profile, name, title, "<pre>" + html.escape(text) + "</pre>", 180 + 32 * len(text.splitlines()))


if __name__ == "__main__":
    main()
