import zmq
import msgpack
import time
import threading

context = zmq.Context()

socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

servers = {}  # { nome: { "rank": int, "last_heartbeat": float } }
rank_counter = 0
lock = threading.Lock()

HEARTBEAT_TIMEOUT = 30

def cleanup_loop():
    while True:
        time.sleep(5)
        now = time.time()
        with lock:
            to_remove = [
                name for name, info in servers.items()
                if now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT
            ]
            for name in to_remove:
                print(f"[REFERENCIA] Servidor '{name}' removido por timeout de heartbeat.", flush=True)
                del servers[name]

cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
cleanup_thread.start()

print("[REFERENCIA] Iniciado na porta 5559.", flush=True)

while True:
    raw = socket.recv()
    msg = msgpack.unpackb(raw, raw=False)
    action = msg.get("action")
    clock_recv = msg.get("clock", 0)

    print(f"[REFERENCIA] RECV | action={action} | clock={clock_recv}", flush=True)

    with lock:
        if action == "register":
            name = msg.get("name", "")
            if not name:
                resp = {"status": "error", "message": "Nome obrigatorio", "clock": clock_recv}
            elif name in servers:
                rank = servers[name]["rank"]
                servers[name]["last_heartbeat"] = time.time()
                resp = {"status": "ok", "rank": rank, "clock": clock_recv}
                print(f"[REFERENCIA] Servidor '{name}' re-registrado com rank {rank}.", flush=True)
            else:
                rank_counter += 1
                servers[name] = {"rank": rank_counter, "last_heartbeat": time.time()}
                resp = {"status": "ok", "rank": rank_counter, "clock": clock_recv}
                print(f"[REFERENCIA] Servidor '{name}' registrado com rank {rank_counter}.", flush=True)

        elif action == "list":
            server_list = [
                {"name": n, "rank": info["rank"]}
                for n, info in servers.items()
            ]
            resp = {"status": "ok", "servers": server_list, "clock": clock_recv}

        elif action == "heartbeat":
            name = msg.get("name", "")
            if name in servers:
                servers[name]["last_heartbeat"] = time.time()
                # Parte 4: não retorna mais a hora — coordenador assume essa responsabilidade
                resp = {"status": "ok", "clock": clock_recv}
                print(f"[REFERENCIA] Heartbeat de '{name}' recebido.", flush=True)
            else:
                resp = {"status": "error", "message": f"Servidor '{name}' nao encontrado", "clock": clock_recv}

        else:
            resp = {"status": "error", "message": f"Acao desconhecida: {action}", "clock": clock_recv}

    print(f"[REFERENCIA] SEND | status={resp['status']}", flush=True)
    socket.send(msgpack.packb(resp))