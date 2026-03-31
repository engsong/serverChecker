from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from src.utils import ensure_dir, now_display, slugify


DEFAULT_TIMEOUT_MS = 4000
DEFAULT_VIEWPORT_WIDTH = 1600
DEFAULT_VIEWPORT_HEIGHT = 900
DEFAULT_SELECTOR_TIMEOUT_MS = 300

DEFAULT_USERNAME_SELECTORS = [
    "input[type='email']",
    "input[name='email']",
    "input[id='email']",
    "input[name='username']",
    "input[id='username']",
    "input[name='user']",
    "input[id='user']",
    "input[type='text']",
]

DEFAULT_PASSWORD_SELECTORS = [
    "input[type='password']",
    "input[name='password']",
    "input[id='password']",
]

DEFAULT_SUBMIT_SELECTORS = [
    "button[type='submit']",
    "input[type='submit']",
    "button:has-text('Login')",
    "button:has-text('Log in')",
    "button:has-text('Sign in')",
    "button:has-text('Submit')",
]


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _env_key_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(value).strip().upper())
    return token.strip("_")


def _default_secret_env_names(target_name: str, site_name: str, secret_name: str) -> list[str]:
    candidates: list[str] = []
    target_token = _env_key_token(target_name)
    site_token = _env_key_token(site_name)

    for candidate in (
        f"{target_token}_{secret_name}" if target_token else "",
        f"{site_token}_{target_token}_{secret_name}" if site_token and target_token else "",
        f"WEB_{target_token}_{secret_name}" if target_token else "",
    ):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    return candidates


def _resolve_secret(
    item: dict[str, Any],
    value_key: str,
    env_key: str,
    *,
    secret_name: str,
    target_name: str,
    site_name: str,
) -> str:
    direct_value = str(item.get(value_key, "") or "").strip()
    if direct_value:
        return direct_value

    env_name = str(item.get(env_key, "") or "").strip()
    if env_name:
        env_value = str(os.getenv(env_name, "")).strip()
        if env_value:
            return env_value

    for candidate in _default_secret_env_names(target_name, site_name, secret_name):
        env_value = str(os.getenv(candidate, "")).strip()
        if env_value:
            return env_value

    return ""


def _normalize_selectors(value: Any, defaults: list[str]) -> list[str]:
    if isinstance(value, str) and value.strip():
        return [value.strip()]

    if isinstance(value, list):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        if normalized:
            return normalized

    return defaults


def _wait_for_visible(page, selectors: list[str], timeout_ms: int):
    per_selector_timeout = max(150, min(timeout_ms, DEFAULT_SELECTOR_TIMEOUT_MS))
    last_error: Exception | None = None

    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=per_selector_timeout)
            return locator, selector
        except Exception as exc:
            last_error = exc

    if last_error:
        raise last_error

    raise RuntimeError("No selector matched.")


def _is_still_login_form(page, username_selector: str, password_selector: str) -> bool:
    try:
        page.locator(username_selector).first.wait_for(state="visible", timeout=700)
        page.locator(password_selector).first.wait_for(state="visible", timeout=700)
        return True
    except Exception:
        return False


def _page_has_login_form(page, web_item: dict[str, Any], timeout_ms: int) -> bool:
    username_selectors = _normalize_selectors(web_item.get("username_selector"), DEFAULT_USERNAME_SELECTORS)
    password_selectors = _normalize_selectors(web_item.get("password_selector"), DEFAULT_PASSWORD_SELECTORS)

    try:
        _wait_for_visible(page, username_selectors, timeout_ms)
        _wait_for_visible(page, password_selectors, timeout_ms)
        return True
    except Exception:
        return False


def _wait_after_navigation(
    page,
    web_item: dict[str, Any],
    timeout_ms: int,
    apply_capture_delay: bool,
) -> None:
    success_wait_selector = str(web_item.get("success_wait_selector", "") or "").strip()
    pre_screenshot_wait_ms = _as_int(web_item.get("pre_screenshot_wait_ms"), 3000)

    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass

    try:
        page.wait_for_function(
            "() => document.readyState === 'complete'",
            timeout=timeout_ms,
        )
    except Exception:
        pass

    try:
        page.locator("body").first.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        pass

    if success_wait_selector and success_wait_selector != "body":
        try:
            page.locator(success_wait_selector).first.wait_for(state="visible", timeout=timeout_ms)
        except Exception:
            pass

    if apply_capture_delay and pre_screenshot_wait_ms > 0:
        page.wait_for_timeout(pre_screenshot_wait_ms)


def _perform_login(
    page,
    web_item: dict[str, Any],
    timeout_ms: int,
    *,
    username: str,
    password: str,
) -> tuple[str, str, str]:
    if not username or not password:
        raise RuntimeError("Login is required but username/password is missing.")

    username_selectors = _normalize_selectors(web_item.get("username_selector"), DEFAULT_USERNAME_SELECTORS)
    password_selectors = _normalize_selectors(web_item.get("password_selector"), DEFAULT_PASSWORD_SELECTORS)
    submit_selectors = _normalize_selectors(web_item.get("submit_selector"), DEFAULT_SUBMIT_SELECTORS)

    username_locator, matched_username_selector = _wait_for_visible(page, username_selectors, timeout_ms)
    password_locator, matched_password_selector = _wait_for_visible(page, password_selectors, timeout_ms)

    username_locator.fill(username)
    password_locator.fill(password)

    submit_locator, matched_submit_selector = _wait_for_visible(page, submit_selectors, timeout_ms)
    submit_locator.click()

    return matched_username_selector, matched_password_selector, matched_submit_selector


def _inject_capture_overlay(page, captured_at: str) -> None:
    overlay_text = f"Captured: {captured_at}"
    page.evaluate(
        """
        (text) => {
          const oldNode = document.getElementById('__server_checker_capture_stamp__');
          if (oldNode) {
            oldNode.remove();
          }

          const stamp = document.createElement('div');
          stamp.id = '__server_checker_capture_stamp__';
          stamp.textContent = text;
          stamp.style.position = 'fixed';
          stamp.style.right = '18px';
          stamp.style.bottom = '18px';
          stamp.style.zIndex = '2147483647';
          stamp.style.padding = '10px 16px';
          stamp.style.borderRadius = '12px';
          stamp.style.background = 'rgba(0, 0, 0, 0.80)';
          stamp.style.color = '#ffffff';
          stamp.style.fontFamily = 'Arial, sans-serif';
          stamp.style.fontSize = '18px';
          stamp.style.fontWeight = '800';
          stamp.style.boxShadow = '0 12px 32px rgba(0, 0, 0, 0.40)';
          stamp.style.pointerEvents = 'none';
          stamp.style.userSelect = 'none';
          stamp.style.letterSpacing = '0.2px';
          document.body.appendChild(stamp);
        }
        """,
        overlay_text,
    )


def execute_web_check(
    web_item: dict[str, Any],
    run_id: str,
    webshots_dir: Path,
    auth_states_dir: Path,
) -> dict[str, Any]:
    site_name = str(web_item.get("site") or "WEB").strip() or "WEB"
    target_name = str(web_item.get("name") or "web").strip() or "web"
    target_url = str(web_item.get("url") or "").strip()
    login_required = _as_bool(web_item.get("login_required"), False)
    username = _resolve_secret(
        web_item,
        "username",
        "username_env",
        secret_name="USERNAME",
        target_name=target_name,
        site_name=site_name,
    )
    password = _resolve_secret(
        web_item,
        "password",
        "password_env",
        secret_name="PASSWORD",
        target_name=target_name,
        site_name=site_name,
    )
    has_credentials = bool(username and password)

    timeout_ms = _as_int(web_item.get("timeout_ms"), DEFAULT_TIMEOUT_MS)
    viewport_width = _as_int(web_item.get("viewport_width"), DEFAULT_VIEWPORT_WIDTH)
    viewport_height = _as_int(web_item.get("viewport_height"), DEFAULT_VIEWPORT_HEIGHT)
    headless = _as_bool(web_item.get("headless"), True)
    full_page = _as_bool(web_item.get("full_page"), False)
    reuse_storage_state = _as_bool(web_item.get("reuse_storage_state"), False)

    target_slug = slugify(target_name)

    shot_dir = ensure_dir(webshots_dir / target_slug)
    auth_dir = ensure_dir(auth_states_dir)

    screenshot_file = shot_dir / f"{run_id}__{target_slug}.png"
    storage_state_file = auth_dir / f"{target_slug}.json"
    captured_at = now_display()

    result = {
        "name": target_name,
        "site": site_name,
        "url": target_url,
        "login_required": login_required,
        "status": "FAIL",
        "message": "",
        "generated_at": captured_at,
        "captured_at": captured_at,
        "screenshot_file": "",
        "final_url": "",
    }

    if not target_url:
        result["message"] = "Missing target url."
        return result

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=headless,
                args=["--ignore-certificate-errors"],
            )

            context_kwargs: dict[str, Any] = {
                "ignore_https_errors": True,
                "viewport": {
                    "width": viewport_width,
                    "height": viewport_height,
                },
            }

            if reuse_storage_state and storage_state_file.exists():
                context_kwargs["storage_state"] = str(storage_state_file)

            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            page.goto(target_url, wait_until="domcontentloaded", timeout=timeout_ms)
            _wait_after_navigation(
                page=page,
                web_item=web_item,
                timeout_ms=timeout_ms,
                apply_capture_delay=not login_required,
            )

            login_form_detected = _page_has_login_form(
                page=page,
                web_item=web_item,
                timeout_ms=min(timeout_ms, 900),
            )
            used_username_selector = ""
            used_password_selector = ""
            auto_login_used = False

            if login_required or (login_form_detected and has_credentials):
                auto_login_used = not login_required and login_form_detected and has_credentials
                used_username_selector, used_password_selector, _ = _perform_login(
                    page,
                    web_item,
                    timeout_ms,
                    username=username,
                    password=password,
                )

                post_login_url = str(web_item.get("post_login_url") or "").strip()
                if post_login_url:
                    page.goto(post_login_url, wait_until="domcontentloaded", timeout=timeout_ms)

                _wait_after_navigation(
                    page=page,
                    web_item=web_item,
                    timeout_ms=timeout_ms,
                    apply_capture_delay=True,
                )

                if _is_still_login_form(page, used_username_selector, used_password_selector):
                    raise RuntimeError("Login form is still visible after submit. Login may have failed.")

                if reuse_storage_state:
                    context.storage_state(path=str(storage_state_file))
            elif login_form_detected:
                result["message"] = (
                    "Page appears to require login. Set login_required: true and provide "
                    "username/password, username_env/password_env, or "
                    "<TARGET_NAME>_USERNAME/<TARGET_NAME>_PASSWORD in .env."
                )
                context.close()
                browser.close()
                return result

            _inject_capture_overlay(page, captured_at)
            page.wait_for_timeout(200)
            page.screenshot(
                path=str(screenshot_file),
                full_page=full_page,
            )
            result["screenshot_file"] = str(screenshot_file)
            result["final_url"] = page.url
            result["status"] = "PASS"
            result["message"] = (
                "Screenshot captured successfully after auto-login."
                if auto_login_used
                else "Screenshot captured successfully."
            )

            context.close()
            browser.close()

    except PlaywrightTimeoutError:
        result["message"] = f"Timeout after {timeout_ms}ms"
    except Exception as exc:
        result["message"] = str(exc)

    return result
