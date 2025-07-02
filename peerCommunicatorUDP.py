from socket import *
import threading
import pickle
import time
from constMP import *
from requests import get

# === Configurações globais ===
PEERS = []
myself = None
N = None

lamport_clock = 0
lock = threading.Lock()
pending_messages = []
acks = {}  # chave = (timestamp, sender): set of peers
logList = []

# === Funções de Relógio de Lamport ===
def increment_clock():
    global lamport_clock
    with lock:
        lamport_clock += 1
        return lamport_clock

def update_clock(received):
    global lamport_clock
    with lock:
        lamport_clock = max(lamport_clock, received) + 1
        return lamport_clock

# === Funções de entrega ===
def deliver_messages():
    global pending_messages
    delivered = []
    for msg in pending_messages:
        key = (msg["timestamp"], msg["sender"])
        if len(acks.get(key, set())) == N:
            print(f"[DELIVERED] {msg}")
            logList.append((msg["sender"], msg["msg_id"]))
            delivered.append(msg)
    for msg in delivered:
        pending_messages.remove(msg)

# === Handler de recebimento ===
class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        while True:
            msgPack, _ = self.sock.recvfrom(4096)
            msg = pickle.loads(msgPack)

            if msg["type"] == "DATA":
                update_clock(msg["timestamp"])
                pending_messages.append(msg)
                pending_messages.sort(key=lambda m: (m["timestamp"], m["sender"]))

                key = (msg["timestamp"], msg["sender"])
                if key not in acks:
                    acks[key] = set()
                acks[key].add(myself)

                # envia ACK
                ack_msg = {
                    "type": "ACK",
                    "timestamp": increment_clock(),
                    "msg_timestamp": msg["timestamp"],
                    "msg_sender": msg["sender"],
                    "from": myself
                }
                ackPack = pickle.dumps(ack_msg)
                for peer in PEERS:
                    sendSocket.sendto(ackPack, (peer, PEER_UDP_PORT))

            elif msg["type"] == "ACK":
                key = (msg["msg_timestamp"], msg["msg_sender"])
                if key not in acks:
                    acks[key] = set()
                acks[key].add(msg["from"])

            deliver_messages()

# === Funções auxiliares ===
def get_public_ip():
    return get('https://api.ipify.org').content.decode('utf8')

def register_and_fetch_peers():
    global PEERS, N
    ip = get_public_ip()
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps({"op": "register", "ipaddr": ip, "port": PEER_UDP_PORT}))
    clientSock.close()

    time.sleep(1)
    clientSock = socket(AF_INET, SOCK_STREAM)
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.send(pickle.dumps({"op": "list"}))
    msg = clientSock.recv(2048)
    PEERS = pickle.loads(msg)
    N = len(PEERS)
    clientSock.close()

# === Início da execução ===
register_and_fetch_peers()
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
sendSocket = socket(AF_INET, SOCK_DGRAM)
msgHandler = MsgHandler(recvSocket)
msgHandler.start()

my_ip = get_public_ip()
myself = PEERS.index(my_ip) if my_ip in PEERS else 0

# Recebe start do servidor
serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)
print('Aguardando sinal de início...')
conn, addr = serverSock.accept()
msgPack = conn.recv(1024)
start_info = pickle.loads(msgPack)
myself = start_info[0]
nMsgs = start_info[1]
conn.send(pickle.dumps(f'Peer process {myself} started.'))
conn.close()

# Envia mensagens
for i in range(nMsgs):
    time.sleep(0.1)
    msg = {
        "type": "DATA",
        "timestamp": increment_clock(),
        "sender": myself,
        "msg_id": i
    }
    msgPack = pickle.dumps(msg)
    for peer in PEERS:
        sendSocket.sendto(msgPack, (peer, PEER_UDP_PORT))

# Espera entrega
while len(logList) < nMsgs * N:
    time.sleep(0.1)

# Salva log
with open(f'logfile{myself}.log', 'w') as f:
    f.write(str(logList))

# Envia log para o servidor
clientSock = socket(AF_INET, SOCK_STREAM)
clientSock.connect((SERVER_ADDR, SERVER_PORT))
clientSock.send(pickle.dumps(logList))
clientSock.close()
print("Log enviado ao servidor.")
