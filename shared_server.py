import sys
import os
import logging
import paramiko
import subprocess
from dotenv import load_dotenv
from typing import Any
from flask import Flask, request, jsonify

load_dotenv()
app = Flask(__name__)
note_requests: dict[str, Any] = {}
comment_list_requests: dict[str, Any] = {}

logger = logging.getLogger()
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s %(module)s: %(message)s",datefmt=r"%H:%M:%S")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger.addHandler(handler)

if os.getenv('TARGET_DEVICE_TYPE') == '1':
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_ip = os.getenv('SSH_IP')
    if not ssh_ip:
        raise ValueError("SSH_IP environment variable is required")
    ssh_port = os.getenv('SSH_PORT')
    if not ssh_port:
        raise ValueError("SSH_PORT environment variable is required")
    ssh.connect(
        ssh_ip,
        port=int(ssh_port),
        username=os.getenv('SSH_USERNAME'),
        password=os.getenv('SSH_PASSWORD')
    )
else:
    ssh = None

def home_page(connected_ssh_client: paramiko.SSHClient | None = None):
    if os.getenv('TARGET_DEVICE_TYPE') == '0':
        subprocess.run(["adb", "shell", "am", "start", "-d", "xhsdiscover://home"])
    elif os.getenv('TARGET_DEVICE_TYPE') == '1':
        if connected_ssh_client:
            _, _, _ = connected_ssh_client.exec_command(
                "uiopen xhsdiscover://home"
            )
        else:
            subprocess.run(["uiopen", "xhsdiscover://home"])

@app.route("/open_note/<noteId>", methods=["GET"])
def open_note(noteId: str):
    anchorCommentId = request.args.get('anchorCommentId', '')
    if os.getenv('TARGET_DEVICE_TYPE') == '0':
        subprocess.run(["adb", "shell", "am", "start", "-d", f"xhsdiscover://item/{noteId}" + (f"?anchorCommentId={anchorCommentId}" if anchorCommentId else '')])
    elif os.getenv('TARGET_DEVICE_TYPE') == '1':
        if ssh:
            _, _, _ = ssh.exec_command(
                f"uiopen xhsdiscover://item/{noteId}" + (f"?anchorCommentId={anchorCommentId}" if anchorCommentId else '')
            )
        else:
            subprocess.run(["uiopen", f"xhsdiscover://item/{noteId}" + (f"?anchorCommentId={anchorCommentId}" if anchorCommentId else '')])
    return jsonify({"status": "success"})

@app.route("/set_note", methods=["POST"])
def set_note():
    data = request.json
    if data is None:
        return jsonify({"status": "error", "message": "No data provided"}), 400
    note_id = data["note_id"]
    note_requests[note_id] = {
        "url": data["url"],
        "data": data["data"]
    }
    logger.info(f"Note set: {note_id}, {data['url']}")
    return jsonify({"status": "ok"})

@app.route("/set_comment_list", methods=["POST"])
def set_comment_list():
    data = request.json
    if data is None:
        return jsonify({"status": "error", "message": "No data provided"}), 400
    note_id = data["note_id"]
    comment_list_requests[note_id] = {
        "url": data["url"],
        "data": data["data"]
    }
    logger.info(f"Comment list set: {note_id}, {data['url']}")
    return jsonify({"status": "ok"})

@app.route("/get_note/<note_id>")
def get_note(note_id: str):
    json_data = jsonify(note_requests.get(note_id, {}))
    del note_requests[note_id]  # Remove after fetching
    logger.info(f"Note fetched: {note_id}")
    return json_data

@app.route("/get_comment_list/<note_id>")
def get_comment_list(note_id: str):
    json_data = jsonify(comment_list_requests.get(note_id, {}))
    del comment_list_requests[note_id]  # Remove after fetching
    logger.info(f"Comment list fetched: {note_id}")
    return json_data

if __name__ == "__main__":
    port = os.getenv("FLASK_SERVER_PORT")
    app.run(port=int(port) if port else 5001)
