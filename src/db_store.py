from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

try:
    import mysql.connector
except ImportError:  # pragma: no cover - optional runtime dependency
    mysql = None

from src.utils import safe_string, slugify


DISPLAY_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _database_url() -> str:
    return (
        os.getenv("SERVER_CHECKER_DATABASE_URL", "").strip()
        or os.getenv("DATABASE_URL", "").strip()
    )


def _env_mysql_connection_kwargs() -> dict[str, Any] | None:
    database_name = os.getenv("MYSQL_DATABASE", "").strip()
    if not database_name:
        return None

    user = os.getenv("MYSQL_USER", "").strip()
    if not user:
        raise RuntimeError("MYSQL_USER is missing.")

    host = os.getenv("MYSQL_HOST", "127.0.0.1").strip() or "127.0.0.1"
    raw_port = os.getenv("MYSQL_PORT", "3306").strip() or "3306"

    try:
        port = int(raw_port)
    except ValueError as exc:
        raise RuntimeError(f"MYSQL_PORT must be a valid integer, got: {raw_port}") from exc

    return {
        "host": host,
        "port": port,
        "user": user,
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": database_name,
        "charset": "utf8mb4",
        "use_unicode": True,
    }


def _json_string(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _parse_display_time(value: str) -> datetime:
    text = safe_string(value).strip()
    if not text:
        return datetime.now()

    return datetime.strptime(text, DISPLAY_TIME_FORMAT)


def _overall_status(run_result: dict[str, Any]) -> str:
    has_service_failure = any(
        service.get("status") != "PASS"
        for host in run_result.get("hosts", [])
        for service in host.get("services", [])
    )
    has_web_failure = any(
        item.get("status") != "PASS" for item in run_result.get("web_checks", [])
    )
    return "FAIL" if has_service_failure or has_web_failure else "PASS"


def _mysql_connection_kwargs(database_url: str) -> dict[str, Any]:
    parsed = urlparse(database_url)
    if not parsed.scheme.lower().startswith("mysql"):
        raise RuntimeError(
            "SERVER_CHECKER_DATABASE_URL must use a MySQL URL, for example mysql://user:pass@127.0.0.1:3306/server_checker"
        )

    database_name = parsed.path.lstrip("/")
    if not database_name:
        raise RuntimeError("MySQL database URL is missing a database name.")

    return {
        "host": parsed.hostname or "127.0.0.1",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
        "database": database_name,
        "charset": "utf8mb4",
        "use_unicode": True,
    }


def _resolved_connection_kwargs() -> dict[str, Any] | None:
    database_url = _database_url()
    if database_url:
        return _mysql_connection_kwargs(database_url)

    return _env_mysql_connection_kwargs()


def database_config_error() -> str:
    try:
        connection_kwargs = _resolved_connection_kwargs()
    except Exception as exc:
        return str(exc)

    if connection_kwargs:
        return ""

    return (
        "MySQL config is missing. Set SERVER_CHECKER_DATABASE_URL or MYSQL_HOST, "
        "MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE."
    )


class MySQLRunStore:
    def __init__(self, connection_kwargs: dict[str, Any]) -> None:
        if mysql is None:
            raise RuntimeError(
                "Database persistence requires mysql-connector-python. Run `pip install -r requirements.txt` first."
            )

        self.connection_kwargs = connection_kwargs

    def persist_run(
        self,
        run_result: dict[str, Any],
        site_reports: list[tuple[str, Path, Path]],
        web_summary_path: Path | None,
    ) -> None:
        generated_at = _parse_display_time(run_result.get("generated_at", ""))
        run_key = safe_string(run_result.get("run_id")).strip()
        total_hosts = len(run_result.get("hosts", []))
        total_services = sum(len(host.get("services", [])) for host in run_result.get("hosts", []))
        total_passed = sum(
            int(service.get("passed", 0))
            for host in run_result.get("hosts", [])
            for service in host.get("services", [])
        )
        total_failed = sum(
            int(service.get("failed", 0))
            for host in run_result.get("hosts", [])
            for service in host.get("services", [])
        )
        total_web_checks = len(run_result.get("web_checks", []))
        total_web_passed = sum(
            1 for item in run_result.get("web_checks", []) if item.get("status") == "PASS"
        )
        total_web_failed = sum(
            1 for item in run_result.get("web_checks", []) if item.get("status") != "PASS"
        )

        conn = mysql.connector.connect(**self.connection_kwargs)
        try:
            cursor = conn.cursor()
            try:
                run_id = self._upsert_run(
                    cursor=cursor,
                    run_key=run_key,
                    generated_at=generated_at,
                    run_result=run_result,
                    web_summary_path=web_summary_path,
                    total_hosts=total_hosts,
                    total_services=total_services,
                    total_passed=total_passed,
                    total_failed=total_failed,
                    total_web_checks=total_web_checks,
                    total_web_passed=total_web_passed,
                    total_web_failed=total_web_failed,
                )

                self._reset_run_children(cursor=cursor, run_id=run_id)

                site_ids: dict[str, int] = {}
                host_ids: dict[tuple[int, str], int] = {}

                for host in run_result.get("hosts", []):
                    site_name = safe_string(host.get("site") or "UNKNOWN").strip() or "UNKNOWN"
                    site_id = site_ids.setdefault(site_name, self._upsert_site(cursor, site_name))

                    host_key = (site_id, safe_string(host.get("host")).strip())
                    if host_key not in host_ids:
                        host_ids[host_key] = self._upsert_host(
                            cursor=cursor,
                            site_id=site_id,
                            host_item=host,
                        )

                    host_id = host_ids[host_key]

                    for service in host.get("services", []):
                        service_id = self._upsert_service(
                            cursor=cursor,
                            host_id=host_id,
                            service_item=service,
                        )

                        service_result_id = self._insert_service_result(
                            cursor=cursor,
                            run_id=run_id,
                            site_id=site_id,
                            host_id=host_id,
                            service_id=service_id,
                            generated_at=generated_at,
                            host_item=host,
                            service_item=service,
                        )

                        for step_order, step in enumerate(service.get("checks", []), start=1):
                            self._insert_service_check_step(
                                cursor=cursor,
                                service_result_id=service_result_id,
                                step_order=step_order,
                                step=step,
                            )

                for site_name, report_path, summary_png in site_reports:
                    normalized_site = safe_string(site_name or "UNKNOWN").strip() or "UNKNOWN"
                    site_id = site_ids.setdefault(
                        normalized_site, self._upsert_site(cursor, normalized_site)
                    )
                    self._insert_site_report(
                        cursor=cursor,
                        run_id=run_id,
                        site_id=site_id,
                        report_path=report_path,
                        summary_png=summary_png,
                    )

                for web_item in run_result.get("web_checks", []):
                    site_name = safe_string(web_item.get("site") or "WEB").strip() or "WEB"
                    site_id = site_ids.setdefault(site_name, self._upsert_site(cursor, site_name))
                    web_target_id = self._upsert_web_target(
                        cursor=cursor,
                        site_id=site_id,
                        web_item=web_item,
                    )
                    self._insert_web_result(
                        cursor=cursor,
                        run_id=run_id,
                        site_id=site_id,
                        web_target_id=web_target_id,
                        generated_at=generated_at,
                        web_item=web_item,
                    )

            finally:
                cursor.close()

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _upsert_run(
        self,
        cursor,
        run_key: str,
        generated_at: datetime,
        run_result: dict[str, Any],
        web_summary_path: Path | None,
        total_hosts: int,
        total_services: int,
        total_passed: int,
        total_failed: int,
        total_web_checks: int,
        total_web_passed: int,
        total_web_failed: int,
    ) -> int:
        cursor.execute(
            """
            INSERT INTO check_runs (
                run_key,
                generated_at,
                source,
                overall_status,
                total_hosts,
                total_services,
                total_passed,
                total_failed,
                total_web_checks,
                total_web_passed,
                total_web_failed,
                web_summary_report_path,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                generated_at = VALUES(generated_at),
                source = VALUES(source),
                overall_status = VALUES(overall_status),
                total_hosts = VALUES(total_hosts),
                total_services = VALUES(total_services),
                total_passed = VALUES(total_passed),
                total_failed = VALUES(total_failed),
                total_web_checks = VALUES(total_web_checks),
                total_web_passed = VALUES(total_web_passed),
                total_web_failed = VALUES(total_web_failed),
                web_summary_report_path = VALUES(web_summary_report_path),
                raw_payload = VALUES(raw_payload),
                id = LAST_INSERT_ID(id)
            """,
            (
                run_key,
                generated_at,
                "python-runner",
                _overall_status(run_result),
                total_hosts,
                total_services,
                total_passed,
                total_failed,
                total_web_checks,
                total_web_passed,
                total_web_failed,
                str(web_summary_path) if web_summary_path else "",
                _json_string(run_result),
            ),
        )
        return int(cursor.lastrowid)

    def _reset_run_children(self, cursor, run_id: int) -> None:
        cursor.execute("DELETE FROM site_run_reports WHERE check_run_id = %s", (run_id,))
        cursor.execute("DELETE FROM web_check_results WHERE check_run_id = %s", (run_id,))
        cursor.execute("DELETE FROM service_results WHERE check_run_id = %s", (run_id,))

    def _upsert_site(self, cursor, site_name: str) -> int:
        cursor.execute(
            """
            INSERT INTO sites (site_name, site_slug, updated_at)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                site_slug = VALUES(site_slug),
                updated_at = NOW(),
                id = LAST_INSERT_ID(id)
            """,
            (site_name, slugify(site_name or "UNKNOWN")),
        )
        return int(cursor.lastrowid)

    def _upsert_host(self, cursor, site_id: int, host_item: dict[str, Any]) -> int:
        host_address = safe_string(host_item.get("host")).strip()
        display_name = safe_string(host_item.get("display_name") or host_address).strip() or host_address
        prompt_host = (
            safe_string(host_item.get("prompt_host") or display_name or host_address).strip()
            or host_address
        )

        cursor.execute(
            """
            INSERT INTO hosts (
                site_id,
                host_address,
                display_name,
                prompt_host,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                display_name = VALUES(display_name),
                prompt_host = VALUES(prompt_host),
                updated_at = NOW(),
                id = LAST_INSERT_ID(id)
            """,
            (site_id, host_address, display_name, prompt_host),
        )
        return int(cursor.lastrowid)

    def _upsert_service(self, cursor, host_id: int, service_item: dict[str, Any]) -> int:
        service_name = safe_string(service_item.get("service_name") or service_item.get("name")).strip()
        profile_name = safe_string(service_item.get("profile_name")).strip()
        protocol = safe_string(service_item.get("protocol") or "ssh").strip() or "ssh"
        ssh_port = int(service_item.get("ssh_port") or 22)
        username = safe_string(service_item.get("username")).strip()

        cursor.execute(
            """
            INSERT INTO services (
                host_id,
                service_name,
                service_slug,
                check_profile_name,
                protocol,
                ssh_port,
                username,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                service_slug = VALUES(service_slug),
                check_profile_name = VALUES(check_profile_name),
                protocol = VALUES(protocol),
                ssh_port = VALUES(ssh_port),
                username = VALUES(username),
                updated_at = NOW(),
                id = LAST_INSERT_ID(id)
            """,
            (
                host_id,
                service_name,
                slugify(service_name or "service"),
                profile_name,
                protocol,
                ssh_port,
                username,
            ),
        )
        return int(cursor.lastrowid)

    def _insert_site_report(
        self,
        cursor,
        run_id: int,
        site_id: int,
        report_path: Path,
        summary_png: Path,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO site_run_reports (
                check_run_id,
                site_id,
                report_html_path,
                summary_screenshot_file
            )
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                report_html_path = VALUES(report_html_path),
                summary_screenshot_file = VALUES(summary_screenshot_file)
            """,
            (run_id, site_id, str(report_path), str(summary_png)),
        )

    def _insert_service_result(
        self,
        cursor,
        run_id: int,
        site_id: int,
        host_id: int,
        service_id: int,
        generated_at: datetime,
        host_item: dict[str, Any],
        service_item: dict[str, Any],
    ) -> int:
        cursor.execute(
            """
            INSERT INTO service_results (
                check_run_id,
                site_id,
                host_id,
                service_id,
                host_address,
                host_display_name,
                service_name,
                profile_name,
                status,
                passed_count,
                failed_count,
                protocol,
                ssh_port,
                raw_log,
                connection_error,
                log_file,
                service_report_html_path,
                service_screenshot_file,
                generated_at,
                raw_payload
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                run_id,
                site_id,
                host_id,
                service_id,
                safe_string(service_item.get("host") or host_item.get("host")).strip(),
                safe_string(host_item.get("display_name") or host_item.get("host")).strip(),
                safe_string(service_item.get("service_name")).strip(),
                safe_string(service_item.get("profile_name")).strip(),
                safe_string(service_item.get("status")).strip(),
                int(service_item.get("passed", 0)),
                int(service_item.get("failed", 0)),
                safe_string(service_item.get("protocol") or "ssh").strip() or "ssh",
                int(service_item.get("ssh_port") or 22),
                safe_string(service_item.get("raw_log")),
                safe_string(service_item.get("connection_error")).strip(),
                safe_string(service_item.get("log_file")).strip(),
                safe_string(service_item.get("service_report_html")).strip(),
                safe_string(service_item.get("service_screenshot_file")).strip(),
                generated_at,
                _json_string(service_item),
            ),
        )
        return int(cursor.lastrowid)

    def _insert_service_check_step(
        self,
        cursor,
        service_result_id: int,
        step_order: int,
        step: dict[str, Any],
    ) -> None:
        cursor.execute(
            """
            INSERT INTO service_check_steps (
                service_result_id,
                step_order,
                step_name,
                command,
                display_command,
                prompt_dir,
                ok,
                exit_code,
                duration_sec,
                stdout,
                stderr,
                runner_error,
                notes,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                service_result_id,
                step_order,
                safe_string(step.get("name")).strip(),
                safe_string(step.get("command")).strip(),
                safe_string(step.get("display_command")).strip(),
                safe_string(step.get("prompt_dir") or "~").strip() or "~",
                bool(step.get("ok", False)),
                int(step.get("exit_code", 0) or 0),
                float(step.get("duration_sec", 0) or 0),
                safe_string(step.get("stdout")),
                safe_string(step.get("stderr")),
                safe_string(step.get("error")).strip(),
                _json_string(step.get("notes", [])),
                _json_string(step),
            ),
        )

    def _upsert_web_target(self, cursor, site_id: int, web_item: dict[str, Any]) -> int:
        target_name = safe_string(web_item.get("name") or "web").strip() or "web"
        target_url = safe_string(web_item.get("url")).strip()
        login_required = bool(web_item.get("login_required", False))

        cursor.execute(
            """
            INSERT INTO web_targets (
                site_id,
                target_name,
                target_slug,
                target_url,
                login_required,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                target_slug = VALUES(target_slug),
                target_url = VALUES(target_url),
                login_required = VALUES(login_required),
                updated_at = NOW(),
                id = LAST_INSERT_ID(id)
            """,
            (
                site_id,
                target_name,
                slugify(target_name),
                target_url,
                login_required,
            ),
        )
        return int(cursor.lastrowid)

    def _insert_web_result(
        self,
        cursor,
        run_id: int,
        site_id: int,
        web_target_id: int,
        generated_at: datetime,
        web_item: dict[str, Any],
    ) -> None:
        captured_at = (
            _parse_display_time(web_item.get("captured_at", ""))
            if web_item.get("captured_at")
            else generated_at
        )

        cursor.execute(
            """
            INSERT INTO web_check_results (
                check_run_id,
                site_id,
                web_target_id,
                target_name,
                target_url,
                final_url,
                status,
                login_required,
                message,
                captured_at,
                screenshot_file,
                web_report_html_path,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                site_id,
                web_target_id,
                safe_string(web_item.get("name") or "web").strip() or "web",
                safe_string(web_item.get("url")).strip(),
                safe_string(web_item.get("final_url")).strip(),
                safe_string(web_item.get("status")).strip(),
                bool(web_item.get("login_required", False)),
                safe_string(web_item.get("message")).strip(),
                captured_at,
                safe_string(web_item.get("screenshot_file")).strip(),
                safe_string(web_item.get("web_report_html")).strip(),
                _json_string(web_item),
            ),
        )


def persist_run_result(
    run_result: dict[str, Any],
    site_reports: list[tuple[str, Path, Path]],
    web_summary_path: Path | None = None,
) -> bool:
    connection_kwargs = _resolved_connection_kwargs()
    if not connection_kwargs:
        return False

    store = MySQLRunStore(connection_kwargs=connection_kwargs)
    store.persist_run(
        run_result=run_result,
        site_reports=site_reports,
        web_summary_path=web_summary_path,
    )
    return True
