from __future__ import annotations

import sys
from pathlib import Path

from src.check_executor import build_config_error_result, execute_host_service_checks
from src.report_builder import (
    write_html_report,
    write_service_artifacts,
    write_web_artifacts,
    write_web_summary_report,
)
from src.screenshot import capture_html_screenshot
from src.db_store import database_config_error, persist_run_result
from src.utils import ensure_dir, load_env_file, load_yaml, now_display, now_timestamp, slugify
from src.web_executor import execute_web_check


def _group_hosts_by_site(run_result: dict) -> dict[str, dict]:
    grouped: dict[str, dict] = {}

    for host in run_result["hosts"]:
        site_name = str(host.get("site") or "UNKNOWN").strip() or "UNKNOWN"
        site_bucket = grouped.setdefault(
            site_name,
            {
                "run_id": run_result["run_id"],
                "generated_at": run_result["generated_at"],
                "hosts": [],
            },
        )
        site_bucket["hosts"].append(host)

    return grouped


def main() -> int:
    base_dir = Path(__file__).resolve().parent.parent
    load_env_file(base_dir / ".env")

    configs_dir = base_dir / "configs"
    output_dir = base_dir / "output"
    logs_dir = output_dir / "logs"
    reports_dir = output_dir / "reports"
    service_reports_dir = reports_dir / "services"
    screenshots_dir = output_dir / "screenshots"
    service_screenshots_dir = screenshots_dir / "services"
    webshots_dir = output_dir / "webshots"
    web_reports_dir = output_dir / "web_reports"
    web_auth_dir = output_dir / "web_auth"

    templates_dir = base_dir / "templates"

    ensure_dir(logs_dir)
    ensure_dir(reports_dir)
    ensure_dir(service_reports_dir)
    ensure_dir(screenshots_dir)
    ensure_dir(service_screenshots_dir)
    ensure_dir(webshots_dir)
    ensure_dir(web_reports_dir)
    ensure_dir(web_auth_dir)
    hosts_config = load_yaml(configs_dir / "hosts.yaml")
    checks_config = load_yaml(configs_dir / "checks.yaml")

    hosts = hosts_config.get("hosts", [])
    web_targets = hosts_config.get("web_targets", [])
    profiles = checks_config.get("profiles", {})
    default_timeout_sec = int(checks_config.get("default_timeout_sec", 20))

    run_id = now_timestamp()
    run_result = {
        "run_id": run_id,
        "generated_at": now_display(),
        "hosts": [],
        "web_checks": [],
    }

    has_failure = False

    for host_item in hosts:
        host_result = {
            "host": host_item.get("host", ""),
            "display_name": host_item.get("display_name", host_item.get("host", "")),
            "site": host_item.get("site", ""),
            "services": [],
            "status": "PASS",
            "passed": 0,
            "failed": 0,
        }

        for service in host_item.get("services", []):
            profile_name = service.get("check_profile", "")
            profile = profiles.get(profile_name)

            if not profile:
                service_result = build_config_error_result(
                    host_item=host_item,
                    service=service,
                    message=f"Missing profile in checks.yaml: {profile_name}",
                )
            else:
                service_result = execute_host_service_checks(
                    host_item=host_item,
                    service=service,
                    profile=profile,
                    default_timeout_sec=default_timeout_sec,
                )

            host_result["services"].append(service_result)
            host_result["passed"] += service_result["passed"]
            host_result["failed"] += service_result["failed"]

            if service_result["failed"] > 0:
                has_failure = True

        if host_result["failed"] > 0:
            host_result["status"] = "FAIL"

        run_result["hosts"].append(host_result)

    write_service_artifacts(
        run_result=run_result,
        logs_dir=logs_dir,
        service_reports_dir=service_reports_dir,
        run_id=run_id,
    )

    for host in run_result["hosts"]:
        host_site_slug = slugify(host.get("site", "") or "UNKNOWN")

        for service in host["services"]:
            service_slug = slugify(service["service_name"])
            host_slug = slugify(service["host"])

            service_png_dir = ensure_dir(service_screenshots_dir / host_site_slug / service_slug)
            service_png = service_png_dir / f"{run_id}__{host_slug}__{service_slug}.png"

            capture_html_screenshot(
                html_path=Path(service["service_report_html"]),
                image_path=service_png,
                width=1600,
                height=1200,
            )
            service["service_screenshot_file"] = str(service_png)

    for web_item in web_targets:
        web_result = execute_web_check(
            web_item=web_item,
            run_id=run_id,
            webshots_dir=webshots_dir,
            auth_states_dir=web_auth_dir,
        )
        run_result["web_checks"].append(web_result)

        if web_result["status"] != "PASS":
            has_failure = True

    write_web_artifacts(
        web_results=run_result["web_checks"],
        web_reports_dir=web_reports_dir,
        run_id=run_id,
    )

    web_summary_path = write_web_summary_report(
        web_results=run_result["web_checks"],
        reports_dir=web_reports_dir,
        run_id=run_id,
    )

    site_run_results = _group_hosts_by_site(run_result)
    site_reports: list[tuple[str, Path, Path]] = []

    for site_name, site_result in site_run_results.items():
        site_slug = slugify(site_name or "UNKNOWN")
        site_reports_dir = ensure_dir(reports_dir / site_slug)
        site_screenshots_dir = ensure_dir(screenshots_dir / site_slug)

        report_path = write_html_report(
            run_result=site_result,
            template_path=templates_dir / "report.html.j2",
            reports_dir=site_reports_dir,
            run_id=run_id,
        )

        summary_png = site_screenshots_dir / f"{run_id}__{site_slug}__server_check_report.png"
        capture_html_screenshot(
            html_path=report_path,
            image_path=summary_png,
            width=1600,
            height=1200,
        )

        site_reports.append((site_name, report_path, summary_png))

    db_persisted = False
    db_error = ""

    try:
        db_persisted = persist_run_result(
            run_result=run_result,
            site_reports=site_reports,
            web_summary_path=web_summary_path,
        )
    except Exception as exc:
        db_error = str(exc)

    if not db_persisted and not db_error:
        db_error = database_config_error()

    total_hosts = len(run_result["hosts"])
    total_services = sum(len(host["services"]) for host in run_result["hosts"])
    total_failed = sum(service["failed"] for host in run_result["hosts"] for service in host["services"])
    total_passed = sum(service["passed"] for host in run_result["hosts"] for service in host["services"])
    total_web_checks = len(run_result["web_checks"])
    total_web_passed = sum(1 for item in run_result["web_checks"] if item["status"] == "PASS")
    total_web_failed = sum(1 for item in run_result["web_checks"] if item["status"] != "PASS")

    print("=" * 80)
    print("Server check completed")
    print(f"Run ID          : {run_id}")
    print(f"Generated At    : {run_result['generated_at']}")
    print(f"Total Hosts     : {total_hosts}")
    print(f"Total Services  : {total_services}")
    print(f"Total Passed    : {total_passed}")
    print(f"Total Failed    : {total_failed}")
    print(f"Web Checks      : {total_web_checks}")
    print(f"Web Passed      : {total_web_passed}")
    print(f"Web Failed      : {total_web_failed}")
    print(f"Service Logs    : {logs_dir}")
    print(f"Service HTML    : {service_reports_dir}")
    print(f"Service PNG     : {service_screenshots_dir}")
    print(f"Web Reports     : {web_reports_dir}")
    print(f"Web Screenshots : {webshots_dir}")
    print(f"Web Summary     : {web_summary_path}")
    print(f"Database Saved  : {'YES' if db_persisted else 'NO'}")

    if db_error:
        print(f"Database Error  : {db_error}", file=sys.stderr)

    for site_name, report_path, summary_png in site_reports:
        print(f"[{site_name}] Summary Report : {report_path}")
        print(f"[{site_name}] Summary PNG    : {summary_png}")

    print("=" * 80)

    if not db_persisted:
        return 2

    return 1 if has_failure else 0
