from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from src.utils import ensure_dir, slugify, write_text


def calculate_totals(run_result: dict[str, Any]) -> dict[str, int]:
    total_hosts = len(run_result["hosts"])
    total_services = sum(len(host["services"]) for host in run_result["hosts"])
    total_checks = sum(len(service["checks"]) for host in run_result["hosts"] for service in host["services"])
    total_passed = sum(service["passed"] for host in run_result["hosts"] for service in host["services"])
    total_failed = sum(service["failed"] for host in run_result["hosts"] for service in host["services"])

    return {
        "total_hosts": total_hosts,
        "total_services": total_services,
        "total_checks": total_checks,
        "total_passed": total_passed,
        "total_failed": total_failed,
    }


def _split_lines(text: str) -> list[str]:
    if text == "":
        return []

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.rstrip("\n")
    return normalized.split("\n")


def _prompt_html(username: str, prompt_host: str, prompt_dir: str, display_command: str) -> str:
    return (
        '<div class="line">'
        '<span class="prompt-bracket">[</span>'
        f'<span class="prompt-user">{escape(username)}</span>'
        '<span class="prompt-at">@</span>'
        f'<span class="prompt-host">{escape(prompt_host)}</span>'
        '<span class="prompt-space"> </span>'
        f'<span class="prompt-dir">{escape(prompt_dir)}</span>'
        '<span class="prompt-bracket">]</span>'
        '<span class="prompt-dollar">$</span>'
        f'<span class="prompt-cmd"> {escape(display_command)}</span>'
        "</div>"
    )


def _plain_line_html(text: str, css_class: str = "output") -> str:
    if text == "":
        return '<div class="line output">&nbsp;</div>'
    return f'<div class="line {css_class}">{escape(text)}</div>'


def _render_terminal_html(service: dict[str, Any]) -> str:
    username = str(service.get("username", ""))
    prompt_host = str(service.get("prompt_host", service.get("display_name", service.get("host", ""))))
    terminal_entries = service.get("terminal_entries", []) or []

    fragments: list[str] = []

    if terminal_entries:
        for entry in terminal_entries:
            prompt_dir = str(entry.get("prompt_dir") or "~")
            display_command = str(entry.get("display_command") or "")
            fragments.append(_prompt_html(username, prompt_host, prompt_dir, display_command))

            stdout = entry.get("stdout", "") or ""
            stderr = entry.get("stderr", "") or ""

            for line in _split_lines(stdout):
                fragments.append(_plain_line_html(line, "output"))

            for line in _split_lines(stderr):
                fragments.append(_plain_line_html(line, "error"))
    else:
        for line in _split_lines(service.get("raw_log", "")):
            fragments.append(_plain_line_html(line, "output"))

    terminal_body = "\n".join(fragments)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{escape(str(service.get("service_name", "")))}</title>
  <style>
    html {{
      margin: 0;
      padding: 0;
      background: #000000;
      width: max-content;
      height: max-content;
    }}

    body {{
      margin: 0;
      padding: 0;
      background: #000000;
      width: max-content;
      height: max-content;
      display: inline-block;
      overflow: hidden;
      color: #d2d2d2;
      font-family: Consolas, "Lucida Console", "Courier New", monospace;
      font-size: 13px;
      line-height: 1.25;
      font-weight: 500;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      text-rendering: optimizeLegibility;
    }}

    .terminal {{
      display: inline-block;
      width: max-content;
      height: max-content;
      box-sizing: border-box;
      padding: 6px 8px;
      background: #000000;
    }}

    .line {{
      white-space: pre;
      margin: 0;
      padding: 0;
      color: #d2d2d2;
    }}

    .prompt-user,
    .prompt-host,
    .prompt-at,
    .prompt-bracket,
    .prompt-dollar {{
      color: #d2d2d2;
    }}

    .prompt-dir,
    .prompt-cmd {{
      color: #d2d2d2;
    }}

    .output {{
      color: #d2d2d2;
    }}

    .error {{
      color: #ff8a80;
    }}
  </style>
</head>
<body>
  <div class="terminal">
{terminal_body}
  </div>
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
            write_text(html_path, _render_terminal_html(service))

            service["artifact_site_dir"] = site_slug
            service["artifact_service_dir"] = service_slug
            service["log_file"] = str(log_path)
            service["service_report_html"] = str(html_path)


def write_html_report(
    run_result: dict[str, Any],
    template_path: Path,
    reports_dir: Path,
    run_id: str,
) -> Path:
    ensure_dir(reports_dir)

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    template = env.get_template(template_path.name)

    totals = calculate_totals(run_result)
    html = template.render(
        run_id=run_id,
        generated_at=run_result["generated_at"],
        hosts=run_result["hosts"],
        totals=totals,
    )

    report_path = reports_dir / f"{run_id}__server_check_report.html"
    write_text(report_path, html)
    return report_path
