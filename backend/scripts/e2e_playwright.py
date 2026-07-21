#!/usr/bin/env python3
"""
Automated E2E validation — Real Gemini Live Translate.

Pipeline validated:
  Browser mic → LiveKit → Audio Agent → Pipeline → StreamingSpeechProcessor
  → StreamingSession → GeminiLiveTranslateRuntime → Google SDK
  → receive() → StreamingPartialTranslationEvent + StreamingTranslationAudioEvent
  → LiveKit data channel → Frontend → Transcript + PCM playback

Requirements:
  - Backend running on http://localhost:8000
  - Frontend (Next.js) running on http://localhost:3001 (or 3000)
  - samples/english_sample.wav present (used for fake microphone in headless Chromium)
  - Google API key configured in backend/.env

Evidence collected to output/e2e/:
  - e2e_evidence.json  (full summary with pass/fail per acceptance criterion)
  - e2e_receiver.png   (screenshot of receiver page when translation arrived)
  - e2e_sender.png     (screenshot of sender page)
  - e2e_receiver_timeout.png / e2e_sender_timeout.png (on failure)
  - playwright_trace/  (Playwright trace for timeline analysis)
  - gemini_debug.log   (if E2E_DEBUG_GEMINI=1)

Exit codes:
  0  all acceptance criteria pass
  1  one or more acceptance criteria failed
  2  pre-conditions not met (WAV missing, backend unreachable)
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import requests
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
FRONTEND_URL  = os.environ.get("FRONTEND_URL",  "http://localhost:3001")
BACKEND_URL   = os.environ.get("BACKEND_URL",   "http://localhost:8000")
ROOM_NAME     = os.environ.get("E2E_ROOM",      "e2e-test-room")
PARTICIPANT_A = os.environ.get("E2E_SENDER",    "e2e-participant-alpha")
PARTICIPANT_B = os.environ.get("E2E_RECEIVER",  "e2e-participant-beta")

REPO_ROOT  = Path(__file__).resolve().parents[2]
WAV_PATH   = REPO_ROOT / "samples" / "english_sample.wav"
OUTPUT_DIR = REPO_ROOT / "output" / "e2e"

# How long (seconds) to wait for the first streaming translation token on the receiver
TRANSLATION_TIMEOUT_S = 90

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[E2E {ts}] {msg}", flush=True)


def _wait_backend_healthy(timeout_s: int = 90) -> bool:
    """Poll the backend health endpoint until it returns ok or timeout."""
    url = BACKEND_URL + "/health"
    _log(f"Waiting for backend health at {url}")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200 and r.json().get("status") == "ok":
                _log("Backend is healthy.")
                return True
        except Exception:
            pass
        time.sleep(1)
    _log("Backend health check timed out.")
    return False


def _check_frontend_reachable() -> bool:
    """Quick connectivity check for the frontend server."""
    try:
        r = requests.get(FRONTEND_URL, timeout=5)
        return r.status_code < 500
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Page helpers
# ---------------------------------------------------------------------------

async def _wait_and_screenshot(page, name: str) -> None:
    try:
        await page.screenshot(path=str(OUTPUT_DIR / name))
        _log(f"Screenshot saved: {name}")
    except Exception as e:
        _log(f"Screenshot failed ({name}): {e}")


async def _page_console_errors(logs: List[str]) -> List[str]:
    return [l for l in logs if "error" in l.lower() or "failed" in l.lower()]


async def _connect_participant(page, room: str, identity: str, label: str) -> bool:
    """
    Fill room/identity, wait for Start Session button to be enabled, click it,
    and wait until the connected UI appears.
    Returns True on success.
    """
    try:
        # Fill form fields using Playwright's fill (triggers React onChange)
        await page.wait_for_selector("#room-name-input", timeout=15000)
        await page.fill("#room-name-input", room)
        await page.fill("#identity-input", identity)

        _log(f"[{label}] Filled room={room}, identity={identity}. Waiting for button to be enabled...")

        # The button is enabled when backendConnected=true in the React app.
        # The frontend polls /health every 5 s. In headless mode this should complete < 10 s.
        await page.wait_for_selector(
            'button[type="submit"]:not([disabled])',
            timeout=30000
        )

        _log(f"[{label}] Start Session button is enabled. Clicking...")
        await page.click('button[type="submit"]')

        # Wait until we see the Leave Session button (= connected state)
        await page.wait_for_selector("text=Leave Session", timeout=60000)
        _log(f"[{label}] Connected to room.")
        return True

    except PlaywrightTimeout as e:
        _log(f"[{label}] TIMEOUT during connect: {e}")
        return False
    except Exception as e:
        _log(f"[{label}] ERROR during connect: {e}")
        return False


# ---------------------------------------------------------------------------
# Main E2E
# ---------------------------------------------------------------------------

async def run_e2e() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -----------------------------------------------------------------------
    # Stage 0: Pre-condition checks
    # -----------------------------------------------------------------------
    _log("=== Stage 0: Pre-condition checks ===")

    if not WAV_PATH.exists():
        _log(f"FAIL: WAV file not found at {WAV_PATH}. "
             "Please provide a short English WAV at samples/english_sample.wav")
        return 2

    _log(f"WAV file: {WAV_PATH} ({WAV_PATH.stat().st_size} bytes)")

    if not _wait_backend_healthy():
        _log("FAIL: Backend is not reachable. Start the backend first.")
        return 2

    if not _check_frontend_reachable():
        _log(f"FAIL: Frontend at {FRONTEND_URL} is not reachable. Start the frontend first.")
        return 2

    _log("Pre-conditions OK.")

    # -----------------------------------------------------------------------
    # Stage 1: Trigger agent start for this E2E room
    # -----------------------------------------------------------------------
    _log("=== Stage 1: Start audio agent for room ===")
    try:
        r = requests.post(
            BACKEND_URL + "/api/audio/agent/start",
            json={"room_name": ROOM_NAME},
            timeout=10
        )
        _log(f"Agent start response: {r.status_code} {r.text.strip()[:200]}")
    except Exception as e:
        _log(f"WARNING: Could not start agent via API: {e}")

    # -----------------------------------------------------------------------
    # Stage 2: Launch Chromium with fake mic
    # -----------------------------------------------------------------------
    _log("=== Stage 2: Launch Chromium ===")
    evidence: Dict[str, Any] = {
        "room": ROOM_NAME,
        "participant_a": PARTICIPANT_A,
        "participant_b": PARTICIPANT_B,
        "wav_path": str(WAV_PATH),
        "stages": {},
        "console_a": [],
        "console_b": [],
        "network_responses": [],
        "criteria": {}
    }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--use-fake-device-for-media-stream",
                "--use-fake-ui-for-media-stream",
                f"--use-file-for-fake-audio-capture={str(WAV_PATH)}",
                # Disable web-security to ensure fetch() to localhost:8000 is not blocked
                # by same-site policies in headless mode (dev-only, never use in production)
                "--disable-web-security",
                "--allow-running-insecure-content",
                "--autoplay-policy=no-user-gesture-required",
            ]
        )

        # -----------------------------------------------------------------------
        # Stage 3: Connect Participant A (sender)
        # -----------------------------------------------------------------------
        _log("=== Stage 3: Connect Participant A (sender) ===")

        ctx_a = await browser.new_context(permissions=["microphone"])
        await ctx_a.grant_permissions(["microphone"], origin=FRONTEND_URL)
        page_a = await ctx_a.new_page()

        # Enable Playwright tracing for both contexts
        await ctx_a.tracing.start(screenshots=True, snapshots=True, sources=False)

        page_a.on("console", lambda msg: evidence["console_a"].append(f"[{msg.type}] {msg.text}"))
        page_a.on("pageerror", lambda err: evidence["console_a"].append(f"[pageerror] {err}"))

        def on_response_a(resp):
            try:
                if any(p in resp.url for p in ("/api/", "/health")):
                    evidence["network_responses"].append({
                        "participant": "A",
                        "url": resp.url,
                        "status": resp.status,
                        "time": time.time()
                    })
            except Exception:
                pass

        page_a.on("response", on_response_a)

        await page_a.goto(FRONTEND_URL, wait_until="domcontentloaded")
        await _wait_and_screenshot(page_a, "stage3_a_loaded.png")

        connected_a = await _connect_participant(page_a, ROOM_NAME, PARTICIPANT_A, "A")
        evidence["stages"]["participant_a_connected"] = connected_a
        await _wait_and_screenshot(page_a, "stage3_a_connected.png")

        if not connected_a:
            _log("FAIL: Participant A could not connect. Collecting diagnostics...")
            cons_errors = await _page_console_errors(evidence["console_a"])
            _log(f"Console errors for A: {cons_errors[:10]}")
            evidence["stages"]["abort_reason"] = "participant_a_connect_failed"
            await ctx_a.tracing.stop(path=str(OUTPUT_DIR / "trace_a.zip"))
            await ctx_a.close()
            await browser.close()
            _save_evidence(evidence)
            return 1

        # -----------------------------------------------------------------------
        # Stage 4: Connect Participant B (receiver)
        # -----------------------------------------------------------------------
        _log("=== Stage 4: Connect Participant B (receiver) ===")

        ctx_b = await browser.new_context(permissions=["microphone"])
        await ctx_b.grant_permissions(["microphone"], origin=FRONTEND_URL)
        page_b = await ctx_b.new_page()

        await ctx_b.tracing.start(screenshots=True, snapshots=True, sources=False)

        page_b.on("console", lambda msg: evidence["console_b"].append(f"[{msg.type}] {msg.text}"))
        page_b.on("pageerror", lambda err: evidence["console_b"].append(f"[pageerror] {err}"))

        def on_response_b(resp):
            try:
                if any(p in resp.url for p in ("/api/", "/health")):
                    evidence["network_responses"].append({
                        "participant": "B",
                        "url": resp.url,
                        "status": resp.status,
                        "time": time.time()
                    })
            except Exception:
                pass

        page_b.on("response", on_response_b)

        await page_b.goto(FRONTEND_URL, wait_until="domcontentloaded")
        await _wait_and_screenshot(page_b, "stage4_b_loaded.png")

        connected_b = await _connect_participant(page_b, ROOM_NAME, PARTICIPANT_B, "B")
        evidence["stages"]["participant_b_connected"] = connected_b
        await _wait_and_screenshot(page_b, "stage4_b_connected.png")

        if not connected_b:
            _log("FAIL: Participant B could not connect.")
            evidence["stages"]["abort_reason"] = "participant_b_connect_failed"
            await ctx_a.tracing.stop(path=str(OUTPUT_DIR / "trace_a.zip"))
            await ctx_b.tracing.stop(path=str(OUTPUT_DIR / "trace_b.zip"))
            await ctx_a.close()
            await ctx_b.close()
            await browser.close()
            _save_evidence(evidence)
            return 1

        _log("Both participants connected. Waiting for streaming translation on B...")

        # -----------------------------------------------------------------------
        # Stage 5: Wait for streaming translation text on Participant B
        # -----------------------------------------------------------------------
        _log(f"=== Stage 5: Waiting for Spanish transcript on B (timeout={TRANSLATION_TIMEOUT_S}s) ===")
        translation_text = ""
        got_translation_text = False

        try:
            # StreamingPartialTranslationEvent → page.tsx addLog('Streaming Spanish Transcript: ...')
            # → also sets spanishTranslation → renders into <p class="text-indigo-100"> in TranslationCard.
            # Poll for either signal. Do NOT use wait_for_function(any <p> with length>3) —
            # that picks up UI labels immediately. Instead poll specifically.
            _log("Polling for translation evidence (TranslationCard text OR console log)...")

            async def _poll_translation() -> str:
                deadline = time.time() + TRANSLATION_TIMEOUT_S
                while time.time() < deadline:
                    # 1. Check browser console captured by page_b.on('console')
                    if any("Streaming Spanish Transcript" in l for l in evidence["console_b"]):
                        _log("Translation detected via browser console log.")
                        return "[detected via console log]"
                    # 2. Check TranslationCard paragraph specifically
                    try:
                        txt = await page_b.evaluate(
                            """() => {
                                const ps = document.querySelectorAll('p.text-indigo-100');
                                for (const p of ps) {
                                    const t = (p.textContent || '').trim();
                                    if (t.length > 2) return t;
                                }
                                return '';
                            }"""
                        )
                        if txt and len(txt) > 2:
                            return txt
                    except Exception:
                        pass
                    await asyncio.sleep(1.5)
                return ""

            translation_text = await _poll_translation()
            got_translation_text = bool(translation_text)
            evidence["translation_text"] = translation_text
            if got_translation_text:
                _log(f"Translation received on B: '{translation_text[:120]}'")
                await _wait_and_screenshot(page_b, "e2e_receiver.png")
            else:
                _log(f"No translation detected within {TRANSLATION_TIMEOUT_S}s")
                await _wait_and_screenshot(page_b, "e2e_receiver_timeout.png")
                await _wait_and_screenshot(page_a, "e2e_sender_timeout.png")

        except Exception as ex:
            _log(f"Exception during translation wait: {ex}")
            evidence["translation_text"] = ""
            await _wait_and_screenshot(page_b, "e2e_receiver_timeout.png")
            await _wait_and_screenshot(page_a, "e2e_sender_timeout.png")

        await _wait_and_screenshot(page_a, "e2e_sender.png")

        # -----------------------------------------------------------------------
        # Stage 6: Collect developer console logs from B
        # -----------------------------------------------------------------------
        _log("=== Stage 6: Collecting developer console logs ===")
        dev_logs_b: List[str] = []
        try:
            log_items = page_b.locator("text=Streaming Spanish Transcript")
            count = await log_items.count()
            _log(f"Found {count} 'Streaming Spanish Transcript' log entries on B")
            for i in range(min(count, 20)):
                txt = await log_items.nth(i).inner_text()
                dev_logs_b.append(txt)
        except Exception as e:
            _log(f"Could not collect streaming transcript logs: {e}")

        evidence["dev_logs_b"] = dev_logs_b

        # -----------------------------------------------------------------------
        # Stage 7: Check for audio event indicators on B (DOM dev-log panel)
        # addLog() in page.tsx writes to React state → DOM (not browser console).
        # We check the rendered LogsConsole text for "StreamingTranslationAudioEvent received".
        # -----------------------------------------------------------------------
        _log("=== Stage 7: Check audio playback indicators ===")
        got_audio_event = False
        audio_dom_count = 0
        try:
            audio_dom_count = await page_b.locator("text=StreamingTranslationAudioEvent received").count()
            if audio_dom_count > 0:
                got_audio_event = True
                _log(f"Found {audio_dom_count} StreamingTranslationAudioEvent entries in B DOM devLogs.")
        except Exception as _ae:
            _log(f"Audio DOM check failed: {_ae}")
        # Fallback: also check browser console (covers pcmPlayer.onPlaybackStart etc.)
        if not got_audio_event:
            got_audio_event = any(
                "StreamingTranslationAudioEvent" in l
                or "Audio Playback Started" in l
                or "playChunk" in l.lower()
                for l in evidence["console_b"]
            )
        evidence["stages"]["got_audio_event"] = got_audio_event
        evidence["stages"]["audio_dom_count"] = audio_dom_count
        _log(f"Audio event observed on B: {got_audio_event}")

        # -----------------------------------------------------------------------
        # Stage 8: Save Playwright traces
        # -----------------------------------------------------------------------
        _log("=== Stage 8: Saving traces ===")
        try:
            await ctx_a.tracing.stop(path=str(OUTPUT_DIR / "trace_a.zip"))
            await ctx_b.tracing.stop(path=str(OUTPUT_DIR / "trace_b.zip"))
            _log("Traces saved.")
        except Exception as e:
            _log(f"Trace save failed: {e}")

        await ctx_a.close()
        await ctx_b.close()
        await browser.close()

    # -----------------------------------------------------------------------
    # Stage 9: Evaluate acceptance criteria
    # -----------------------------------------------------------------------
    _log("=== Stage 9: Acceptance criteria evaluation ===")
    criteria = {
        "backend_healthy":          True,   # passed Stage 0
        "frontend_reachable":       True,   # passed Stage 0
        "participant_a_connected":  connected_a,
        "participant_b_connected":  connected_b,
        "translation_text_received": got_translation_text,
        "translation_text_non_empty": len(translation_text) > 0,
        "audio_event_received":     got_audio_event,
    }
    evidence["criteria"] = criteria

    all_pass = all(criteria.values())
    evidence["success"] = all_pass

    for k, v in criteria.items():
        status = "PASS" if v else "FAIL"
        _log(f"  [{status}] {k}")

    _log(f"\n{'ALL CRITERIA PASS' if all_pass else 'SOME CRITERIA FAILED'}")

    # -----------------------------------------------------------------------
    # Stage 10: Save evidence
    # -----------------------------------------------------------------------
    _save_evidence(evidence)
    return 0 if all_pass else 1


def _save_evidence(evidence: Dict[str, Any]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    evidence_path = OUTPUT_DIR / "e2e_evidence.json"
    with open(evidence_path, "w", encoding="utf-8") as f:
        json.dump(evidence, f, indent=2, default=str)
    _log(f"Evidence written to: {evidence_path}")


if __name__ == "__main__":
    code = asyncio.run(run_e2e())
    sys.exit(code)

