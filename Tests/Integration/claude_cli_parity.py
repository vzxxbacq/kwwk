"""macOS offline parity check. Build kwwk first and put the real claude on PATH."""

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class MessagesEndpoint(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.split("?")[0] != "/v1/messages":
            self.send_error(404)
            return

        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        turn = len(self.server.results)
        results = [
            block for message in body["messages"] for block in message["content"]
            if isinstance(block, dict) and block["type"] == "tool_result"
        ]
        self.server.results.append([
            (result["tool_use_id"], result["content"], result.get("is_error", False))
            for result in results
        ])
        if turn > 2:
            self.send_error(400, "Unexpected extra model turn")
            return

        if turn < 2:
            name = ["read", "edit"][turn]
            tools = {tool["name"] for tool in body["tools"]}
            wire_name = name if name in tools else "mcp__kwwk__" + name
            if wire_name not in tools:
                self.send_error(400, "Missing requested tool")
                return
            args = {"path": "fixture.txt"}
            if name == "edit":
                args["edits"] = [{"oldText": "before", "newText": "after"}]
            block = {"type": "tool_use", "name": wire_name, "id": "toolu_" + name, "input": {}}
            delta = {"type": "input_json_delta", "partial_json": json.dumps(args)}
            stop = "tool_use"
        else:
            block = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": "verified"}
            stop = "end_turn"

        message = {"id": "msg_" + str(turn), "role": "assistant", "type": "message", "content": [],
                   "model": body["model"], "usage": {"input_tokens": 4, "output_tokens": 0}}
        events = [
            {"type": "message_start", "message": message},
            {"type": "content_block_start", "index": 0, "content_block": block},
            {"type": "content_block_delta", "index": 0, "delta": delta},
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": stop}, "usage": {"output_tokens": 2}},
            {"type": "message_stop"},
        ]
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for event in events:
            self.wfile.write(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n".encode())

    def log_message(self, *_):
        pass


def run(binary, claude, server, use_cli):
    server.results = []
    address = f"http://127.0.0.1:{server.server_port}"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / ".kwwk").mkdir()
        (root / "fixture.txt").write_text("before")
        wrapper = root / "claude"
        wrapper.write_text(f'#!/bin/sh\nexport ANTHROPIC_BASE_URL={address}\nexec {shlex.quote(claude)} "$@"\n')
        wrapper.chmod(0o700)
        login = "anthropic" if use_cli else "anthropic-api-key"
        store = {login: {"access": "fixture", "refresh": "", "expires": 9999999999999, "extras": {"baseUrl": address}}}
        (root / ".kwwk" / "oauth.json").write_text(json.dumps(store))
        env = {"PATH": tmp + ":" + os.environ["PATH"], "HOME": tmp, "CFFIXED_USER_HOME": tmp}
        route = "claude-cli" if use_cli else "anthropic"
        proc = subprocess.run([binary, "-p", "--model", route + "/claude-sonnet-4-6", "Edit fixture.txt"],
                              env=env, cwd=root, capture_output=True, text=True, timeout=40)
        if proc.returncode:
            raise RuntimeError(proc.stderr)
        assert proc.stdout == "verified\n"
        assert (root / "fixture.txt").read_text() == "after"
        assert [len(results) for results in server.results] == [0, 1, 2]
        assert server.results[1][0] == ("toolu_read", [{"type": "text", "text": "before"}], False)
        return server.results


if __name__ == "__main__":
    if sys.platform != "darwin":
        sys.exit("This fixture uses macOS Foundation's CFFIXED_USER_HOME to isolate the OAuth store.")
    binary = Path(__file__).resolve().parents[2] / ".build/debug/kwwk"
    claude = shutil.which("claude")
    if not binary.exists() or not claude:
        sys.exit("Run swift build --product kwwk and put claude on PATH first.")
    def session(use_cli):
        with ThreadingHTTPServer(("127.0.0.1", 0), MessagesEndpoint) as server:
            threading.Thread(target=server.serve_forever, daemon=True).start()
            try:
                return run(binary, claude, server, use_cli)
            finally:
                server.shutdown()

    with ThreadPoolExecutor(max_workers=2) as pool:
        direct, bridged = pool.map(session, [False, True])
    assert direct == bridged
    print("Concurrent direct and CLI sessions matched across read, edit and final output.")
