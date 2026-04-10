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
    return {"logins": [], "channels": [], "messages": []}

def save_data(data):
    os.makedirs("/data", exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

context = zmq.Context()

rep_socket = context.socket(zmq.REP)
rep_socket.connect("tcp://broker:5556")

pub_socket = context.socket(zmq.PUB)
pub_socket.connect("tcp://pubsub_proxy:5557")

data = load_data()
print(f"[SERVER-{SERVER_ID}] Iniciado e conectado.", flush=True)

while True:
    raw = rep_socket.recv()
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

    elif action == "publish":
        channel = msg.get("channel", "")
        message = msg.get("message", "")
        username = msg.get("username", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time()}
        elif not message:
            resp = {"status": "error", "message": "Mensagem vazia", "timestamp": time.time()}
        else:
            if channel not in data["channels"]:
                data["channels"].append(channel)
                save_data(data)
                print(f"[SERVER-{SERVER_ID}] Canal '{channel}' replicado de outro servidor.", flush=True)
            pub_payload = {
                "channel": channel,
                "message": message,
                "username": username,
                "timestamp": timestamp
            }
            pub_socket.send_multipart([
                channel.encode(),
                msgpack.packb(pub_payload)
            ])
            data["messages"].append(pub_payload)
            save_data(data)
            resp = {"status": "ok", "message": "Mensagem publicada", "timestamp": time.time()}
            print(f"[SERVER-{SERVER_ID}] PUB  | canal={channel} | user={username} | msg={message}", flush=True)

    else:
        resp = {"status": "error", "message": f"Acao desconhecida: {action}", "timestamp": time.time()}

    print(f"[SERVER-{SERVER_ID}] SEND | status={resp['status']} | {resp.get('message', resp.get('channels'))}", flush=True)
    rep_socket.send(msgpack.packb(resp))