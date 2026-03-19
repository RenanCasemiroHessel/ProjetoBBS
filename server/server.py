import zmq
import msgpack
import time
import json
import os

SERVER_ID = os.environ.get("SERVER_ID", "1")
DATA_FILE = f"/data/server_{SERVER_ID}.json"

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {"logins": [], "channels": []}

def save_data(data):
    os.makedirs("/data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.connect("tcp://broker:5556")

data = load_data()
print(f"[SERVER-{SERVER_ID}] Iniciado e conectado ao broker.", flush=True)

while True:
    raw = socket.recv()
    msg = msgpack.unpackb(raw, raw=False)
    action = msg.get("action")
    timestamp = msg.get("timestamp")

    print(f"[SERVER-{SERVER_ID}] RECV | action={action} | user={msg.get('username')} | ts={timestamp}", flush=True)

    if action == "login":
        username = msg.get("username", "")
        if not username:
            resp = {"status": "error", "message": "Username obrigatorio", "timestamp": time.time()}
        else:
            data["logins"].append({"username": username, "timestamp": timestamp})
            save_data(data)
            resp = {"status": "ok", "message": f"Bem-vindo {username}", "timestamp": time.time()}

    elif action == "create_channel":
        channel = msg.get("channel", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time()}
        elif channel in data["channels"]:
            resp = {"status": "error", "message": f"Canal '{channel}' ja existe", "timestamp": time.time()}
        else:
            data["channels"].append(channel)
            save_data(data)
            resp = {"status": "ok", "message": f"Canal '{channel}' criado", "timestamp": time.time()}

    elif action == "list_channels":
        resp = {"status": "ok", "channels": data["channels"], "timestamp": time.time()}

    else:
        resp = {"status": "error", "message": f"Acao desconhecida: {action}", "timestamp": time.time()}

    print(f"[SERVER-{SERVER_ID}] SEND | status={resp['status']} | {resp.get('message', resp.get('channels'))}", flush=True)
    socket.send(msgpack.packb(resp))