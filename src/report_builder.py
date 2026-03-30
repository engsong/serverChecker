from __future__ import annotations

from pathlib import Path
from typing import Any

from src.utils import ensure_dir, slugify, write_text


def _render_hello_world_html() -> str:
    return '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Hello World</title>
  <style>
    :root {
      color-scheme: light;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #ffffff;
      color: #111111;
      font-family: Arial, sans-serif;
    }

    h1 {
      margin: 0;
      font-size: clamp(2.5rem, 8vw, 5rem);
      font-weight: 700;
    }
  </style>
</head>
<body>
  <h1>hello world</h1>
</body>
</html>
'''


def write_service_artifacts(
    run_result: dict[str, Any],
    logs_dir: Path,
    service_reports_dir: Path,
    run_id: str,
) -> None:
    ensure_dir(logs_dir)
    ensure_dir(service_reports_dir)

    for host in run_result["hosts"]:
        for service in host["services"]:
            site_slug = slugify(service.get("site") or host.get("site") or "UNKNOWN")
            service_slug = slugify(service["service_name"])
            host_slug = slugify(service["host"])

            service_log_dir = ensure_dir(logs_dir / site_slug / service_slug)
            service_html_dir = ensure_dir(service_reports_dir / site_slug / service_slug)

            log_path = service_log_dir / f"{run_id}__{host_slug}__{service_slug}.log"
            html_path = service_html_dir / f"{run_id}__{host_slug}__{service_slug}.html"

            write_text(log_path, service["raw_log"])
            write_text(html_path, _render_hello_world_html())

            service["artifact_site_dir"] = site_slug
            service["artifact_service_dir"] = service_slug
            service["log_file"] = str(log_path)
            service["service_report_html"] = str(html_path)


def write_web_artifacts(
    web_results: list[dict[str, Any]],
    web_reports_dir: Path,
    run_id: str,
) -> None:
    ensure_dir(web_reports_dir)

    for web_result in web_results:
        name_slug = slugify(web_result.get("name") or "web")
        item_dir = ensure_dir(web_reports_dir / name_slug)
        html_path = item_dir / f"{run_id}__{name_slug}.html"
        write_text(html_path, _render_hello_world_html())
        web_result["web_report_html"] = str(html_path)


def write_web_summary_report(
    web_results: list[dict[str, Any]],
    reports_dir: Path,
    run_id: str,
) -> Path:
    ensure_dir(reports_dir)

    report_path = reports_dir / f"{run_id}__web_check_report.html"
    write_text(report_path, _render_hello_world_html())
    return report_path


def write_html_report(
    run_result: dict[str, Any],
    reports_dir: Path,
    run_id: str,
) -> Path:
    ensure_dir(reports_dir)

    report_path = reports_dir / f"{run_id}__server_check_report.html"
    write_text(report_path, _render_hello_world_html())
    return report_path
