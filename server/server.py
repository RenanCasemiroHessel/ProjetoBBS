import zmq
import msgpack
import time
import json
import os
import threading

SERVER_ID = os.environ.get("SERVER_ID", "1")
SERVER_NAME = f"server{SERVER_ID}"
DATA_FILE = f"/data/server_{SERVER_ID}.json"

# Relógio lógico
clock = 0
clock_lock = threading.Lock()

def tick():
    global clock
    with clock_lock:
        clock += 1
        return clock

def update_clock(received):
    global clock
    with clock_lock:
        clock = max(clock, received) + 1
        return clock

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

ref_socket = context.socket(zmq.REQ)
ref_socket.connect("tcp://referencia:5559")

# Registrar na referência e obter rank
def register_on_reference():
    c = tick()
    req = {"action": "register", "name": SERVER_NAME, "clock": c, "timestamp": time.time()}
    print(f"[SERVER-{SERVER_ID}] Registrando na referência com clock={c}...", flush=True)
    ref_socket.send(msgpack.packb(req))
    raw = ref_socket.recv()
    resp = msgpack.unpackb(raw, raw=False)
    update_clock(resp.get("clock", 0))
    rank = resp.get("rank", -1)
    print(f"[SERVER-{SERVER_ID}] Rank recebido: {rank}", flush=True)
    return rank

data = load_data()
messages_since_heartbeat = 0
rank = -1

try:
    rank = register_on_reference()
except Exception as e:
    print(f"[SERVER-{SERVER_ID}] Erro ao registrar na referência: {e}", flush=True)

print(f"[SERVER-{SERVER_ID}] Iniciado e conectado. Rank={rank}", flush=True)

def send_heartbeat():
    global messages_since_heartbeat
    try:
        c = tick()
        req = {
            "action": "heartbeat",
            "name": SERVER_NAME,
            "clock": c,
            "timestamp": time.time()
        }
        ref_socket.send(msgpack.packb(req))
        raw = ref_socket.recv()
        resp = msgpack.unpackb(raw, raw=False)
        update_clock(resp.get("clock", 0))

        # Sincronização do relógio físico
        ref_time = resp.get("time")
        if ref_time:
            diff = ref_time - time.time()
            print(f"[SERVER-{SERVER_ID}] Heartbeat OK. Diferença de relógio físico: {diff:.4f}s", flush=True)

        messages_since_heartbeat = 0
    except Exception as e:
        print(f"[SERVER-{SERVER_ID}] Erro no heartbeat: {e}", flush=True)

while True:
    raw = rep_socket.recv()
    msg = msgpack.unpackb(raw, raw=False)
    action = msg.get("action")
    timestamp = msg.get("timestamp")
    clock_recv = msg.get("clock", 0)

    current_clock = update_clock(clock_recv)
    messages_since_heartbeat += 1

    print(f"[SERVER-{SERVER_ID}] RECV | action={action} | user={msg.get('username')} | ts={timestamp} | clock={clock_recv}", flush=True)

    if action == "login":
        username = msg.get("username", "")
        if not username:
            resp = {"status": "error", "message": "Username obrigatorio", "timestamp": time.time(), "clock": tick()}
        else:
            data["logins"].append({"username": username, "timestamp": timestamp})
            save_data(data)
            resp = {"status": "ok", "message": f"Bem-vindo {username}", "timestamp": time.time(), "clock": tick()}

    elif action == "create_channel":
        channel = msg.get("channel", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time(), "clock": tick()}
        elif channel in data["channels"]:
            resp = {"status": "error", "message": f"Canal '{channel}' ja existe", "timestamp": time.time(), "clock": tick()}
        else:
            data["channels"].append(channel)
            save_data(data)
            resp = {"status": "ok", "message": f"Canal '{channel}' criado", "timestamp": time.time(), "clock": tick()}

    elif action == "list_channels":
        resp = {"status": "ok", "channels": data["channels"], "timestamp": time.time(), "clock": tick()}

    elif action == "publish":
        channel = msg.get("channel", "")
        message = msg.get("message", "")
        username = msg.get("username", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time(), "clock": tick()}
        elif not message:
            resp = {"status": "error", "message": "Mensagem vazia", "timestamp": time.time(), "clock": tick()}
        else:
            if channel not in data["channels"]:
                data["channels"].append(channel)
                save_data(data)
                print(f"[SERVER-{SERVER_ID}] Canal '{channel}' replicado de outro servidor.", flush=True)
            c = tick()
            pub_payload = {
                "channel": channel,
                "message": message,
                "username": username,
                "timestamp": timestamp,
                "clock": c
            }
            pub_socket.send_multipart([
                channel.encode(),
                msgpack.packb(pub_payload)
            ])
            data["messages"].append(pub_payload)
            save_data(data)
            resp = {"status": "ok", "message": "Mensagem publicada", "timestamp": time.time(), "clock": c}
            print(f"[SERVER-{SERVER_ID}] PUB | canal={channel} | user={username} | msg={message} | clock={c}", flush=True)

    elif action == "list_servers":
        try:
            c = tick()
            ref_req = {"action": "list", "clock": c, "timestamp": time.time()}
            ref_socket.send(msgpack.packb(ref_req))
            raw_ref = ref_socket.recv()
            ref_resp = msgpack.unpackb(raw_ref, raw=False)
            update_clock(ref_resp.get("clock", 0))
            resp = {"status": "ok", "servers": ref_resp.get("servers", []), "timestamp": time.time(), "clock": tick()}
        except Exception as e:
            resp = {"status": "error", "message": str(e), "timestamp": time.time(), "clock": tick()}

    else:
        resp = {"status": "error", "message": f"Acao desconhecida: {action}", "timestamp": time.time(), "clock": tick()}

    print(f"[SERVER-{SERVER_ID}] SEND | status={resp['status']} | clock={resp['clock']}", flush=True)
    rep_socket.send(msgpack.packb(resp))

    # Heartbeat a cada 10 mensagens
    if messages_since_heartbeat >= 10:
        send_heartbeat()