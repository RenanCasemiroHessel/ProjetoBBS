import zmq

context = zmq.Context()

frontend = context.socket(zmq.ROUTER)  # conect cliente
frontend.bind("tcp://*:5555")

backend = context.socket(zmq.DEALER)   # conect servidor 
backend.bind("tcp://*:5556")

print("[BROKER] Iniciado. Aguardando mensagens...", flush=True)
zmq.proxy(frontend, backend)

frontend.close()
backend.close()
context.close()
