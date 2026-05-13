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

# Estado de eleição
coordinator = None
coordinator_lock = threading.Lock()
rank = -1
known_servers = []

# Socket REQ/REP entre servidores (eleição e sincronização Berkeley)
election_rep = context.socket(zmq.REP)
election_rep_port = 5560 + int(SERVER_ID)
election_rep.bind(f"tcp://*:{election_rep_port}")

# Lock para acesso seguro ao data entre threads
data_lock = threading.Lock()

def get_election_req(target_name):
    target_id = target_name.replace("server", "")
    port = 5560 + int(target_id)
    s = context.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 2000)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(f"tcp://{target_name}:{port}")
    return s

def register_on_reference():
    global rank, known_servers
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

def list_servers_from_reference():
    global known_servers
    c = tick()
    req = {"action": "list", "clock": c, "timestamp": time.time()}
    ref_socket.send(msgpack.packb(req))
    raw = ref_socket.recv()
    resp = msgpack.unpackb(raw, raw=False)
    update_clock(resp.get("clock", 0))
    known_servers = resp.get("servers", [])
    return known_servers

def elect_coordinator():
    global coordinator, known_servers
    print(f"[SERVER-{SERVER_ID}] Iniciando eleição...", flush=True)
    servers = list_servers_from_reference()
    higher = [s for s in servers if s["rank"] > rank and s["name"] != SERVER_NAME]

    if not higher:
        with coordinator_lock:
            coordinator = SERVER_NAME
        announce_coordinator()
        print(f"[SERVER-{SERVER_ID}] Eleito como coordenador.", flush=True)
        return

    got_ok = False
    for s in higher:
        try:
            req_sock = get_election_req(s["name"])
            c = tick()
            req_sock.send(msgpack.packb({"action": "election", "name": SERVER_NAME, "clock": c}))
            raw = req_sock.recv()
            resp = msgpack.unpackb(raw, raw=False)
            update_clock(resp.get("clock", 0))
            if resp.get("status") == "ok":
                got_ok = True
            req_sock.close()
        except Exception as e:
            print(f"[SERVER-{SERVER_ID}] Servidor {s['name']} indisponível na eleição: {e}", flush=True)

    if not got_ok:
        with coordinator_lock:
            coordinator = SERVER_NAME
        announce_coordinator()
        print(f"[SERVER-{SERVER_ID}] Nenhum servidor de rank superior respondeu. Eleito como coordenador.", flush=True)

def announce_coordinator():
    c = tick()
    payload = {"coordinator": SERVER_NAME, "clock": c, "timestamp": time.time()}
    pub_socket.send_multipart([b"servers", msgpack.packb(payload)])
    print(f"[SERVER-{SERVER_ID}] Anunciou-se como coordenador no tópico 'servers'.", flush=True)

def sync_clock_with_coordinator():
    global coordinator
    with coordinator_lock:
        coord = coordinator

    if coord is None:
        print(f"[SERVER-{SERVER_ID}] Sem coordenador para sincronizar relógio.", flush=True)
        elect_coordinator()
        return

    if coord == SERVER_NAME:
        print(f"[SERVER-{SERVER_ID}] Sou o coordenador — sem necessidade de sincronizar.", flush=True)
        return

    try:
        req_sock = get_election_req(coord)
        c = tick()
        req_sock.send(msgpack.packb({"action": "get_time", "name": SERVER_NAME, "clock": c}))
        raw = req_sock.recv()
        resp = msgpack.unpackb(raw, raw=False)
        update_clock(resp.get("clock", 0))
        ref_time = resp.get("time")
        if ref_time:
            diff = ref_time - time.time()
            print(f"[SERVER-{SERVER_ID}] Relógio sincronizado com coordenador '{coord}'. Diferença: {diff:.4f}s", flush=True)
        req_sock.close()
    except Exception as e:
        print(f"[SERVER-{SERVER_ID}] Coordenador '{coord}' indisponível. Iniciando nova eleição.", flush=True)
        with coordinator_lock:
            coordinator = None
        elect_coordinator()

def send_heartbeat():
    global messages_since_heartbeat
    try:
        c = tick()
        req = {"action": "heartbeat", "name": SERVER_NAME, "clock": c, "timestamp": time.time()}
        ref_socket.send(msgpack.packb(req))
        raw = ref_socket.recv()
        resp = msgpack.unpackb(raw, raw=False)
        update_clock(resp.get("clock", 0))
        print(f"[SERVER-{SERVER_ID}] Heartbeat enviado à referência.", flush=True)
        messages_since_heartbeat = 0
    except Exception as e:
        print(f"[SERVER-{SERVER_ID}] Erro no heartbeat: {e}", flush=True)

def election_listener():
    print(f"[SERVER-{SERVER_ID}] Ouvindo eleições na porta {election_rep_port}.", flush=True)
    while True:
        try:
            raw = election_rep.recv()
            msg = msgpack.unpackb(raw, raw=False)
            action = msg.get("action")
            update_clock(msg.get("clock", 0))

            if action == "election":
                c = tick()
                election_rep.send(msgpack.packb({"status": "ok", "clock": c}))
                print(f"[SERVER-{SERVER_ID}] Respondeu OK para eleição de {msg.get('name')}.", flush=True)
                threading.Thread(target=elect_coordinator, daemon=True).start()

            elif action == "get_time":
                c = tick()
                election_rep.send(msgpack.packb({"status": "ok", "time": time.time(), "clock": c}))

            else:
                c = tick()
                election_rep.send(msgpack.packb({"status": "error", "message": "Acao desconhecida", "clock": c}))
        except Exception as e:
            print(f"[SERVER-{SERVER_ID}] Erro no listener de eleição: {e}", flush=True)

def coordinator_subscriber():
    global coordinator
    sub = context.socket(zmq.SUB)
    sub.connect("tcp://pubsub_proxy:5558")
    sub.subscribe(b"servers")
    print(f"[SERVER-{SERVER_ID}] Inscrito no tópico 'servers'.", flush=True)
    while True:
        try:
            sub.recv()
            raw = sub.recv()
            msg = msgpack.unpackb(raw, raw=False)
            new_coord = msg.get("coordinator")
            update_clock(msg.get("clock", 0))
            if new_coord:
                with coordinator_lock:
                    coordinator = new_coord
                print(f"[SERVER-{SERVER_ID}] Novo coordenador: '{new_coord}'.", flush=True)
        except Exception as e:
            print(f"[SERVER-{SERVER_ID}] Erro no subscriber de coordenador: {e}", flush=True)

# ---------------------------------------------------------------
# PARTE 5: Replicação via PUB/SUB
# Cada servidor escuta TODOS os tópicos e persiste mensagens
# que não foram processadas por ele (publicadas por outro servidor)
# ---------------------------------------------------------------
def replication_subscriber():
    sub = context.socket(zmq.SUB)
    sub.connect("tcp://pubsub_proxy:5558")
    sub.subscribe(b"")  # inscreve em todos os tópicos
    print(f"[SERVER-{SERVER_ID}] Replicador inscrito em todos os tópicos.", flush=True)
    while True:
        try:
            topic_bytes = sub.recv()
            raw = sub.recv()
            topic = topic_bytes.decode()

            # Ignora tópico interno de coordenação
            if topic == "servers":
                continue

            msg = msgpack.unpackb(raw, raw=False)
            update_clock(msg.get("clock", 0))

            channel  = msg.get("channel", topic)
            message  = msg.get("message")
            username = msg.get("username")
            ts       = msg.get("timestamp")
            clk      = msg.get("clock")

            if not message:
                continue

            with data_lock:
                # Replica canal se ainda não existe localmente
                if channel not in data["channels"]:
                    data["channels"].append(channel)
                    print(f"[SERVER-{SERVER_ID}] REPLICA canal '{channel}'.", flush=True)

                # Evita duplicatas: checa combinação canal+username+timestamp+clock
                already = any(
                    m.get("channel") == channel and
                    m.get("username") == username and
                    m.get("timestamp") == ts and
                    m.get("clock") == clk
                    for m in data["messages"]
                )

                if not already:
                    entry = {
                        "channel": channel,
                        "message": message,
                        "username": username,
                        "timestamp": ts,
                        "clock": clk
                    }
                    data["messages"].append(entry)
                    save_data(data)
                    print(f"[SERVER-{SERVER_ID}] REPLICA msg | canal={channel} | de={username} | clock={clk}", flush=True)

        except Exception as e:
            print(f"[SERVER-{SERVER_ID}] Erro no replicador: {e}", flush=True)

# ---------------------------------------------------------------

data = load_data()
messages_since_heartbeat = 0
messages_since_sync = 0

threading.Thread(target=election_listener, daemon=True).start()
threading.Thread(target=coordinator_subscriber, daemon=True).start()
threading.Thread(target=replication_subscriber, daemon=True).start()

time.sleep(1)

try:
    rank = register_on_reference()
except Exception as e:
    print(f"[SERVER-{SERVER_ID}] Erro ao registrar na referência: {e}", flush=True)

time.sleep(2)
try:
    elect_coordinator()
except Exception as e:
    print(f"[SERVER-{SERVER_ID}] Erro na eleição inicial: {e}", flush=True)

print(f"[SERVER-{SERVER_ID}] Iniciado e conectado. Rank={rank}", flush=True)

while True:
    raw = rep_socket.recv()
    msg = msgpack.unpackb(raw, raw=False)
    action    = msg.get("action")
    timestamp = msg.get("timestamp")
    clock_recv = msg.get("clock", 0)

    current_clock = update_clock(clock_recv)
    messages_since_heartbeat += 1
    messages_since_sync += 1

    print(f"[SERVER-{SERVER_ID}] RECV | action={action} | user={msg.get('username')} | ts={timestamp} | clock={clock_recv}", flush=True)

    if action == "login":
        username = msg.get("username", "")
        if not username:
            resp = {"status": "error", "message": "Username obrigatorio", "timestamp": time.time(), "clock": tick()}
        else:
            with data_lock:
                data["logins"].append({"username": username, "timestamp": timestamp})
                save_data(data)
            resp = {"status": "ok", "message": f"Bem-vindo {username}", "timestamp": time.time(), "clock": tick()}

    elif action == "create_channel":
        channel = msg.get("channel", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time(), "clock": tick()}
        else:
            with data_lock:
                if channel in data["channels"]:
                    resp = {"status": "error", "message": f"Canal '{channel}' ja existe", "timestamp": time.time(), "clock": tick()}
                else:
                    data["channels"].append(channel)
                    save_data(data)
                    resp = {"status": "ok", "message": f"Canal '{channel}' criado", "timestamp": time.time(), "clock": tick()}

    elif action == "list_channels":
        with data_lock:
            channels = list(data["channels"])
        resp = {"status": "ok", "channels": channels, "timestamp": time.time(), "clock": tick()}

    elif action == "publish":
        channel  = msg.get("channel", "")
        message  = msg.get("message", "")
        username = msg.get("username", "")
        if not channel:
            resp = {"status": "error", "message": "Nome do canal obrigatorio", "timestamp": time.time(), "clock": tick()}
        elif not message:
            resp = {"status": "error", "message": "Mensagem vazia", "timestamp": time.time(), "clock": tick()}
        else:
            c = tick()
            pub_payload = {
                "channel":   channel,
                "message":   message,
                "username":  username,
                "timestamp": timestamp,
                "clock":     c
            }
            pub_socket.send_multipart([channel.encode(), msgpack.packb(pub_payload)])

            # O replication_subscriber de TODOS os servidores vai capturar e persistir
            # O servidor atual também persiste diretamente (sem depender do subscriber)
            with data_lock:
                if channel not in data["channels"]:
                    data["channels"].append(channel)
                already = any(
                    m.get("channel") == channel and
                    m.get("username") == username and
                    m.get("timestamp") == timestamp and
                    m.get("clock") == c
                    for m in data["messages"]
                )
                if not already:
                    data["messages"].append(pub_payload)
                save_data(data)

            resp = {"status": "ok", "message": "Mensagem publicada", "timestamp": time.time(), "clock": c}
            print(f"[SERVER-{SERVER_ID}] PUB | canal={channel} | user={username} | msg={message} | clock={c}", flush=True)

    elif action == "list_servers":
        try:
            servers = list_servers_from_reference()
            resp = {"status": "ok", "servers": servers, "timestamp": time.time(), "clock": tick()}
        except Exception as e:
            resp = {"status": "error", "message": str(e), "timestamp": time.time(), "clock": tick()}

    else:
        resp = {"status": "error", "message": f"Acao desconhecida: {action}", "timestamp": time.time(), "clock": tick()}

    print(f"[SERVER-{SERVER_ID}] SEND | status={resp['status']} | clock={resp['clock']}", flush=True)
    rep_socket.send(msgpack.packb(resp))

    if messages_since_heartbeat >= 10:
        send_heartbeat()

    if messages_since_sync >= 15:
        sync_clock_with_coordinator()
        messages_since_sync = 0