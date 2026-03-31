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


def _render_web_result_html(web_result: dict[str, Any]) -> str:
    site_name = escape(str(web_result.get("site") or "WEB"))
    target_name = escape(str(web_result.get("name") or "web"))
    target_url = escape(str(web_result.get("url") or "-"))
    final_url = escape(str(web_result.get("final_url") or "-"))
    status = escape(str(web_result.get("status") or "UNKNOWN"))
    message = escape(str(web_result.get("message") or ""))
    captured_at = escape(str(web_result.get("captured_at") or web_result.get("generated_at") or "-"))
    login_required = "Yes" if bool(web_result.get("login_required")) else "No"
    status_tone = "#2ad7a7" if str(web_result.get("status")).upper() == "PASS" else "#ff6e82"

    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{target_name}</title>
  <style>
    :root {{
      color-scheme: dark;
      font-family: Arial, sans-serif;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      padding: 40px 24px;
      background:
        radial-gradient(circle at top left, rgba(175, 108, 255, 0.18), transparent 28%),
        linear-gradient(180deg, #070c17 0%, #0c1324 100%);
      color: #f6f8ff;
    }}

    .report {{
      width: min(920px, 100%);
      margin: 0 auto;
      padding: 28px;
      border: 1px solid rgba(156, 173, 221, 0.16);
      border-radius: 24px;
      background: rgba(14, 22, 40, 0.94);
      box-shadow: 0 28px 70px rgba(0, 0, 0, 0.38);
    }}

    .eyebrow {{
      color: #8b96bc;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    h1 {{
      margin: 10px 0 8px;
      font-size: 32px;
      line-height: 1.1;
    }}

    p {{
      margin: 0;
      color: #c2cae7;
      line-height: 1.6;
    }}

    .status {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      margin-top: 18px;
      padding: 0 16px;
      border-radius: 999px;
      border: 1px solid {status_tone};
      color: {status_tone};
      font-size: 13px;
      font-weight: 800;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}

    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
      margin-top: 24px;
    }}

    .card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid rgba(156, 173, 221, 0.14);
      background: rgba(255, 255, 255, 0.03);
      min-width: 0;
    }}

    .card strong {{
      display: block;
      margin-bottom: 8px;
      color: #8b96bc;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .value {{
      color: #f6f8ff;
      font-size: 16px;
      font-weight: 700;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    .message {{
      margin-top: 24px;
      padding: 18px;
      border-radius: 18px;
      border: 1px solid rgba(156, 173, 221, 0.14);
      background: rgba(255, 255, 255, 0.03);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    @media (max-width: 720px) {{
      body {{
        padding: 18px;
      }}

      .report {{
        padding: 20px;
      }}

      .grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <section class=\"report\">
    <span class=\"eyebrow\">Web Check</span>
    <h1>{target_name}</h1>
    <p>Captured browser result for <strong>{site_name}</strong>.</p>
    <div class=\"status\">{status}</div>

    <div class=\"grid\">
      <div class=\"card\">
        <strong>Site</strong>
        <div class=\"value\">{site_name}</div>
      </div>
      <div class=\"card\">
        <strong>Captured At</strong>
        <div class=\"value\">{captured_at}</div>
      </div>
      <div class=\"card\">
        <strong>Target URL</strong>
        <div class=\"value\">{target_url}</div>
      </div>
      <div class=\"card\">
        <strong>Final URL</strong>
        <div class=\"value\">{final_url}</div>
      </div>
      <div class=\"card\">
        <strong>Login Required</strong>
        <div class=\"value\">{login_required}</div>
      </div>
      <div class=\"card\">
        <strong>Screenshot File</strong>
        <div class=\"value\">{escape(str(web_result.get("screenshot_file") or "-"))}</div>
      </div>
    </div>

    <div class=\"message\">
      <strong>Message</strong>
      <p>{message or "-"}</p>
    </div>
  </section>
</body>
</html>
"""


def _render_web_summary_html(web_results: list[dict[str, Any]], run_id: str) -> str:
    total_checks = len(web_results)
    total_passed = sum(1 for item in web_results if str(item.get("status")).upper() == "PASS")
    total_failed = total_checks - total_passed

    rows = []
    for web_result in web_results:
        status = escape(str(web_result.get("status") or "UNKNOWN"))
        status_class = "status-pass" if str(web_result.get("status")).upper() == "PASS" else "status-fail"
        rows.append(
            f"""
            <tr>
              <td>{escape(str(web_result.get("site") or "WEB"))}</td>
              <td>{escape(str(web_result.get("name") or "web"))}</td>
              <td>{escape(str(web_result.get("url") or "-"))}</td>
              <td>{escape(str(web_result.get("final_url") or "-"))}</td>
              <td><span class=\"status-chip {status_class}\">{status}</span></td>
              <td>{escape(str(web_result.get("message") or "-"))}</td>
            </tr>
            """
        )

    table_rows = "\n".join(rows) if rows else (
        """
        <tr>
          <td colspan="6" class="empty-state">No web targets were configured for this run.</td>
        </tr>
        """
    )

    return f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Web Check Summary</title>
  <style>
    :root {{
      color-scheme: dark;
      font-family: Arial, sans-serif;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      padding: 32px 20px;
      background:
        radial-gradient(circle at top left, rgba(175, 108, 255, 0.16), transparent 24%),
        linear-gradient(180deg, #070c17 0%, #0c1324 100%);
      color: #f6f8ff;
    }}

    .report {{
      width: min(1180px, 100%);
      margin: 0 auto;
      padding: 28px;
      border-radius: 26px;
      border: 1px solid rgba(156, 173, 221, 0.16);
      background: rgba(14, 22, 40, 0.94);
      box-shadow: 0 28px 70px rgba(0, 0, 0, 0.38);
    }}

    .eyebrow {{
      color: #8b96bc;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    h1 {{
      margin: 10px 0 8px;
      font-size: 34px;
      line-height: 1.1;
    }}

    p {{
      margin: 0;
      color: #c2cae7;
      line-height: 1.6;
    }}

    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
      margin-top: 24px;
    }}

    .summary-card {{
      padding: 18px;
      border-radius: 18px;
      border: 1px solid rgba(156, 173, 221, 0.14);
      background: rgba(255, 255, 255, 0.03);
    }}

    .summary-card strong {{
      display: block;
      margin-bottom: 10px;
      color: #8b96bc;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }}

    .summary-card span {{
      font-size: 28px;
      font-weight: 800;
      letter-spacing: -0.04em;
    }}

    table {{
      width: 100%;
      margin-top: 24px;
      border-collapse: separate;
      border-spacing: 0;
      overflow: hidden;
      border-radius: 18px;
      border: 1px solid rgba(156, 173, 221, 0.14);
    }}

    th,
    td {{
      padding: 14px 16px;
      text-align: left;
      vertical-align: top;
      border-bottom: 1px solid rgba(156, 173, 221, 0.1);
      font-size: 14px;
      line-height: 1.5;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}

    th {{
      color: #8b96bc;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      background: rgba(255, 255, 255, 0.04);
    }}

    tr:last-child td {{
      border-bottom: 0;
    }}

    .status-chip {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 30px;
      padding: 0 12px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      border: 1px solid currentColor;
    }}

    .status-pass {{
      color: #2ad7a7;
    }}

    .status-fail {{
      color: #ff6e82;
    }}

    .empty-state {{
      text-align: center;
      color: #8b96bc;
      padding: 32px 16px;
    }}

    @media (max-width: 860px) {{
      .summary-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <section class=\"report\">
    <span class=\"eyebrow\">Web Monitor</span>
    <h1>Web check summary</h1>
    <p>Run <strong>{escape(run_id)}</strong> consolidated browser checks and screenshot captures.</p>

    <div class=\"summary-grid\">
      <div class=\"summary-card\">
        <strong>Total checks</strong>
        <span>{total_checks}</span>
      </div>
      <div class=\"summary-card\">
        <strong>Passed</strong>
        <span>{total_passed}</span>
      </div>
      <div class=\"summary-card\">
        <strong>Failed</strong>
        <span>{total_failed}</span>
      </div>
    </div>

    <table>
      <thead>
        <tr>
          <th>Site</th>
          <th>Target</th>
          <th>Target URL</th>
          <th>Final URL</th>
          <th>Status</th>
          <th>Message</th>
        </tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </section>
</body>
</html>
"""


def write_web_artifacts(
    web_results: list[dict[str, Any]],
    web_reports_dir: Path,
    run_id: str,
) -> None:
    ensure_dir(web_reports_dir)

    for web_result in web_results:
        name_slug = slugify(str(web_result.get("name") or "web"))
        item_dir = ensure_dir(web_reports_dir / name_slug)
        html_path = item_dir / f"{run_id}__{name_slug}.html"
        write_text(html_path, _render_web_result_html(web_result))
        web_result["web_report_html"] = str(html_path)


def write_web_summary_report(
    web_results: list[dict[str, Any]],
    reports_dir: Path,
    run_id: str,
) -> Path:
    ensure_dir(reports_dir)

    report_path = reports_dir / f"{run_id}__web_check_report.html"
    write_text(report_path, _render_web_summary_html(web_results, run_id))
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
