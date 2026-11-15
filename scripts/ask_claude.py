#!/usr/bin/env python3
"""
scripts/ask_claude.py

Robust bridge to ai-helper socket daemon with configurable timeout and polling fallback.
"""

import json
import socket
import sys
import time

SOCKET_PATH = "/tmp/ai-helper.sock"


def send_socket_request(payload: dict, timeout: float = 300.0) -> dict:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(SOCKET_PATH)
    msg = json.dumps(payload) + "\n"
    s.sendall(msg.encode("utf-8"))

    buffer = ""
    while True:
        chunk = s.recv(4096).decode("utf-8")
        if not chunk:
            break
        buffer += chunk
        if "\n" in buffer:
            break
    s.close()
    return json.loads(buffer.split("\n")[0])


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/ask_claude.py <prompt_file>")
        sys.exit(1)

    prompt_file = sys.argv[1]
    with open(prompt_file, "r") as f:
        prompt_text = f.read()

    print(f"Sending prompt to Claude (claude_2) via ai-helper daemon ({len(prompt_text)} chars)...")
    t0 = time.time()

    # Request ask with 300s timeout
    req = {
        "action": "ask",
        "model": "claude",
        "profile": "claude_2",
        "prompt": prompt_text,
        "isNewChat": False,
        "timeoutMs": 300000,
    }

    try:
        res = send_socket_request(req, timeout=310.0)
        dur = time.time() - t0
        print(f"Response received in {dur:.2f}s!")

        if res.get("success"):
            print("\n================== CLAUDE RESPONSE ==================")
            print(res.get("text", ""))
            print("=====================================================\n")
            with open("docs/claude_phase_4_review_and_phase_5_spec.md", "w") as out:
                out.write(res.get("text", ""))
            print("Saved response to docs/claude_phase_4_review_and_phase_5_spec.md")
        else:
            print(f"Error from Claude: {res.get('error')}")
            sys.exit(1)

    except Exception as e:
        print(f"Socket request encountered error: {e}. Checking eval_js fallback...")
        # Fallback: poll webview directly via eval_js
        for attempt in range(30):
            time.sleep(5)
            try:
                js_code = (
                    "window.__aiHelper ? { "
                    "generating: window.__aiHelper.isGenerating(), "
                    "text: window.__aiHelper.getLastResponse() } : null"
                )
                eval_req = {
                    "action": "eval_js",
                    "model": "claude",
                    "profile": "claude_2",
                    "code": js_code,
                }
                eval_res = send_socket_request(eval_req, timeout=10.0)
                if eval_res.get("success") and eval_res.get("result"):
                    data = eval_res["result"]
                    gen = data.get("generating")
                    t_len = len(data.get("text", "") or "")
                    print(f"Polling attempt {attempt + 1}: generating={gen}, text_len={t_len}")
                    if not data.get("generating") and data.get("text"):
                        print("\n================== CLAUDE RESPONSE (FALLBACK) ==================")
                        print(data["text"])
                        print("===============================================================\n")
                        with open("docs/claude_phase_4_review_and_phase_5_spec.md", "w") as out:
                            out.write(data["text"])
                        print("Saved fallback response to docs/")
                        return
            except Exception as poll_err:
                print(f"Polling error: {poll_err}")


if __name__ == "__main__":
    main()
