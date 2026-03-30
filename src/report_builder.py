from __future__ import annotations

import os
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


def _relative_asset_path(from_path: Path, to_path: Path) -> str:
    return Path(os.path.relpath(to_path, from_path.parent)).as_posix()


def _render_web_result_html(web_result: dict[str, Any], screenshot_src: str) -> str:
    status = escape(str(web_result.get("status", "UNKNOWN")))
    name = escape(str(web_result.get("name", "")))
    site = escape(str(web_result.get("site", "")))
    url = escape(str(web_result.get("url", "")))
    message = escape(str(web_result.get("message", "")))
    final_url = escape(str(web_result.get("final_url", "")))
    login_required = "Yes" if web_result.get("login_required") else "No"
    captured_at = escape(str(web_result.get("captured_at", "")))

    screenshot_html = ""
    if screenshot_src:
        screenshot_html = f'<img class="shot" src="{escape(screenshot_src)}" alt="{name}" />'

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{name}</title>
  <style>
    body {{
      margin: 0;
      padding: 24px;
      background: #0b1020;
      color: #d9e0ea;
      font-family: Arial, sans-serif;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      text-rendering: optimizeLegibility;
    }}
    .card {{
      max-width: 1400px;
      margin: 0 auto;
      background: #121a2d;
      border: 1px solid #24304f;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.35);
    }}
    .head {{
      padding: 20px 24px;
      border-bottom: 1px solid #24304f;
      background: #101729;
    }}
    .title {{
      margin: 0 0 8px;
      font-size: 24px;
      font-weight: 700;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px 16px;
      font-size: 14px;
      color: #b7c4e5;
    }}
    .body {{
      padding: 24px;
    }}
    .status {{
      display: inline-block;
      padding: 6px 12px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      background: rgba(255, 255, 255, 0.08);
      margin-bottom: 16px;
    }}
    .shot {{
      width: 100%;
      border-radius: 14px;
      border: 1px solid #24304f;
      background: #0a0f1e;
    }}
    .msg {{
      margin-top: 16px;
      color: #9fb0d9;
      font-size: 14px;
      white-space: pre-wrap;
      line-height: 1.55;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="head">
      <h1 class="title">{name}</h1>
      <div class="meta">
        <div><strong>Site:</strong> {site}</div>
        <div><strong>Status:</strong> {status}</div>
        <div><strong>URL:</strong> {url}</div>
        <div><strong>Login Required:</strong> {login_required}</div>
        <div><strong>Final URL:</strong> {final_url}</div>
        <div><strong>Captured At:</strong> {captured_at}</div>
      </div>
    </div>
    <div class="body">
      <div class="status">{status}</div>
      {screenshot_html}
      <div class="msg">{message}</div>
    </div>
  </div>
</body>
</html>
'''


def _render_web_summary_html(web_results: list[dict[str, Any]], report_path: Path) -> str:
    cards: list[str] = []

    for item in web_results:
        screenshot_html = ""
        screenshot_file = str(item.get("screenshot_file", "")).strip()
        if screenshot_file:
            screenshot_path = Path(screenshot_file)
            if screenshot_path.exists():
                relative_src = _relative_asset_path(report_path, screenshot_path)
                screenshot_html = f'<img class="thumb" src="{escape(relative_src)}" alt="{escape(str(item.get("name", "")))}" />'

        report_link_html = ""
        report_html = str(item.get("web_report_html", "")).strip()
        if report_html:
            report_path_item = Path(report_html)
            if report_path_item.exists():
                relative_link = _relative_asset_path(report_path, report_path_item)
                report_link_html = f'<a class="link" href="{escape(relative_link)}" target="_blank">Open detail</a>'

        cards.append(
            f'''
            <div class="card">
              <div class="top">
                <div>
                  <h2>{escape(str(item.get("name", "")))}</h2>
                  <div class="meta">{escape(str(item.get("url", "")))}</div>
                  <div class="meta">Site: {escape(str(item.get("site", "")))}</div>
                  <div class="meta">Captured: {escape(str(item.get("captured_at", "")))}</div>
                </div>
                <div class="badge">{escape(str(item.get("status", "UNKNOWN")))}</div>
              </div>
              {screenshot_html}
              <div class="foot">
                <div class="msg">{escape(str(item.get("message", "")))}</div>
                {report_link_html}
              </div>
            </div>
            '''
        )

    cards_html = "\n".join(cards)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Web Check Summary</title>
  <style>
    body {{
      margin: 0;
      padding: 24px;
      background: #08101d;
      color: #d9e0ea;
      font-family: Arial, sans-serif;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      text-rendering: optimizeLegibility;
    }}
    .wrap {{
      max-width: 1600px;
      margin: 0 auto;
    }}
    .title {{
      margin: 0 0 20px;
      font-size: 32px;
      font-weight: 800;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
      gap: 18px;
    }}
    .card {{
      background: #10192b;
      border: 1px solid #223150;
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.28);
    }}
    .top {{
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .top h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .meta {{
      margin-top: 6px;
      font-size: 13px;
      color: #9db0da;
      word-break: break-all;
    }}
    .badge {{
      flex-shrink: 0;
      padding: 6px 12px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.08);
      font-size: 12px;
      font-weight: 700;
    }}
    .thumb {{
      width: 100%;
      border-radius: 14px;
      border: 1px solid #223150;
      background: #0a1220;
      display: block;
      margin-bottom: 12px;
    }}
    .foot {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }}
    .msg {{
      font-size: 13px;
      color: #aec0e8;
      white-space: pre-wrap;
      line-height: 1.55;
    }}
    .link {{
      color: #8ab4ff;
      text-decoration: none;
      font-weight: 700;
      white-space: nowrap;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">Web Check Summary</h1>
    <div class="grid">
      {cards_html}
    </div>
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

        screenshot_src = ""
        screenshot_file = str(web_result.get("screenshot_file", "")).strip()
        if screenshot_file:
            screenshot_path = Path(screenshot_file)
            if screenshot_path.exists():
                screenshot_src = _relative_asset_path(html_path, screenshot_path)

        write_text(html_path, _render_web_result_html(web_result, screenshot_src))
        web_result["web_report_html"] = str(html_path)


def write_web_summary_report(
    web_results: list[dict[str, Any]],
    reports_dir: Path,
    run_id: str,
) -> Path:
    ensure_dir(reports_dir)

    report_path = reports_dir / f"{run_id}__web_check_report.html"
    html = _render_web_summary_html(web_results, report_path)
    write_text(report_path, html)
    return report_path


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
