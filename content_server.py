import socket
import sys
import threading
import time
import json
import argparse
from collections import defaultdict

KEEPALIVE_INTERVAL = 3    # send keepalive every 3 seconds
KEEPALIVE_TIMEOUT = 9     # timeout after missing 3 keepalives
LSA_INTERVAL = 5          # send LSA every 5 seconds
BUFSIZE = 4096


class ContentServer:
    def __init__(self, config_file):
        self.config = self.parse_config(config_file)
        self.uuid = self.config['uuid']
        self.name = self.config['name']
        self.backend_port = self.config['backend_port']
        self.peers = self.config['peers']

        self.neighbors = {}
        self.neighbor_names = {}

        # link-state database (LSDB)
        self.lsdb = {self.uuid: {'seq_num': 0, 'neighbors': {}}}
        self.seq_num = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', self.backend_port))

        self.lock = threading.Lock()
        self.running = True

    def parse_config(self, config_file):
        config = {}
        peers = {}

        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                if '=' in line:
                    key, value = line.split('=', 1)
                    key, value = key.strip(), value.strip()

                    if key == 'uuid':
                        config['uuid'] = value
                    elif key == 'name':
                        config['name'] = value
                    elif key == 'backend_port':
                        config['backend_port'] = int(value)
                    elif key == 'peer_count':
                        config['peer_count'] = int(value)
                    elif key.startswith('peer_'):
                        parts = []
                        for p in value.split(','):
                            parts.append(p.strip())
                        peer_uuid, peer_host = parts[0], parts[1]
                        peer_port, peer_metric = int(parts[2]), int(parts[3])
                        peers[peer_uuid] = {
                            'host': peer_host,
                            'backend_port': peer_port,
                            'metric': peer_metric
                        }

        config['peers'] = peers
        return config

    def send_message(self, msg, host, port):
        try:
            if host in ['localhost', '127.0.0.1', '']:
                host = 'localhost'
            self.sock.sendto(json.dumps(msg).encode(), (host, port))
        except Exception:
            pass

    def send_keepalive(self):
        msg = {
            'type': 'keepalive',
            'uuid': self.uuid,
            'name': self.name,
            'seq_num': self.seq_num
        }

        with self.lock:
            for peer_uuid, peer_info in self.peers.items():
                self.send_message(msg, peer_info['host'], peer_info['backend_port'])

    def send_lsa(self):
        with self.lock:
            neighbors = {}
            for peer_uuid, peer_info in self.peers.items():
                if peer_uuid in self.neighbors:
                    neighbors[peer_uuid] = peer_info['metric']

            self.seq_num += 1
            self.lsdb[self.uuid] = {
                'seq_num': self.seq_num,
                'neighbors': neighbors
            }

            msg = {
                'type': 'lsa',
                'uuid': self.uuid,
                'name': self.name,
                'seq_num': self.seq_num,
                'neighbors': neighbors
            }

            # flood to active neighbors only
            for peer_uuid, peer_info in self.peers.items():
                if peer_uuid in self.neighbors:
                    self.send_message(msg, peer_info['host'], peer_info['backend_port'])

    def process_keepalive(self, msg, addr):
        peer_uuid = msg['uuid']
        peer_name = msg['name']

        with self.lock:
            if peer_uuid in self.peers:
                self.neighbors[peer_uuid] = {
                    'name': peer_name,
                    'host': self.peers[peer_uuid]['host'],
                    'backend_port': self.peers[peer_uuid]['backend_port'],
                    'metric': self.peers[peer_uuid]['metric'],
                    'last_seen': time.time()
                }
                self.neighbor_names[peer_uuid] = peer_name
            else:
                host, port = addr
                self.peers[peer_uuid] = {
                    'host': host,
                    'backend_port': port,
                    'metric': 1
                }
                self.neighbors[peer_uuid] = {
                    'name': peer_name,
                    'host': host,
                    'backend_port': port,
                    'metric': 1,
                    'last_seen': time.time()
                }
                self.neighbor_names[peer_uuid] = peer_name

    def process_lsa(self, msg, addr):
        peer_uuid = msg['uuid']
        peer_name = msg['name']
        peer_seq = msg['seq_num']
        peer_neighbors = msg['neighbors']

        with self.lock:
            self.neighbor_names[peer_uuid] = peer_name
            should_flood = False

            if peer_uuid not in self.lsdb:
                should_flood = True
            elif peer_seq > self.lsdb[peer_uuid]['seq_num']:
                should_flood = True

            if should_flood:
                self.lsdb[peer_uuid] = {
                    'seq_num': peer_seq,
                    'neighbors': peer_neighbors
                }

                sender_host, sender_port = addr
                for forward_uuid, peer_info in self.peers.items():
                    if forward_uuid in self.neighbors:
                        if not (peer_info['host'] == sender_host and
                                peer_info['backend_port'] == sender_port):
                            self.send_message(msg, peer_info['host'], peer_info['backend_port'])

    def check_neighbor_timeouts(self):
        now = time.time()
        with self.lock:
            to_remove = [
                peer_uuid for peer_uuid, info in self.neighbors.items()
                if now - info['last_seen'] > KEEPALIVE_TIMEOUT
            ]
            for peer_uuid in to_remove:
                del self.neighbors[peer_uuid]

    def receive_loop(self):
        while self.running:
            try:
                self.sock.settimeout(1.0)
                data, addr = self.sock.recvfrom(BUFSIZE)
                msg = json.loads(data.decode())

                if msg['type'] == 'keepalive':
                    self.process_keepalive(msg, addr)
                elif msg['type'] == 'lsa':
                    self.process_lsa(msg, addr)
            except socket.timeout:
                continue
            except Exception:
                pass

    def keepalive_loop(self):
        while self.running:
            self.send_keepalive()
            time.sleep(KEEPALIVE_INTERVAL)

    def lsa_loop(self):
        time.sleep(1)
        while self.running:
            self.send_lsa()
            time.sleep(LSA_INTERVAL)

    def timeout_loop(self):
        while self.running:
            self.check_neighbor_timeouts()
            time.sleep(1)

    def dijkstra(self):
        graph = {node: lsa['neighbors'] for node, lsa in self.lsdb.items()}
        distances = {self.uuid: 0}
        visited = set()

        while len(visited) < len(graph):
            current = None
            min_dist = float('inf')

            for node in graph:
                if node not in visited and distances.get(node, float('inf')) < min_dist:
                    min_dist = distances[node]
                    current = node

            if current is None:
                break

            visited.add(current)
            for neighbor, metric in graph[current].items():
                if neighbor in graph:
                    new_dist = distances[current] + metric
                    if new_dist < distances.get(neighbor, float('inf')):
                        distances[neighbor] = new_dist

        return distances

    def get_uuid(self):
        return {'uuid': self.uuid}

    def get_neighbors(self):
        with self.lock:
            result = {}
            for peer_uuid, info in self.neighbors.items():
                name = info['name']
                host = info['host'] if info['host'] not in ['127.0.0.1', ''] else 'localhost'
                result[name] = {
                    'uuid': peer_uuid,
                    'host': host,
                    'backend_port': info['backend_port'],
                    'metric': info['metric']
                }
            return {'neighbors': result}

    def get_map(self):
        with self.lock:
            result = {}
            distances = self.dijkstra()
            reachable = {}
            for node, dist in distances.items():
                if dist != float('inf'):
                    distances.add(node)
            for node_uuid, lsa in reachable:
                node_name = self.neighbor_names.get(
                    node_uuid, self.name if node_uuid == self.uuid else node_uuid
                )
                neighbors = {}
                for neighbor_uuid, metric in lsa['neighbors'].items():
                    if neighbor_uuid in self.lsdb:
                        neighbor_name = self.neighbor_names.get(neighbor_uuid, neighbor_uuid)
                        neighbors[neighbor_name] = metric
                result[node_name] = neighbors
            return {'map': result}

    def get_rank(self):
        with self.lock:
            distances = self.dijkstra()
            result = {}
            for node_uuid, distance in distances.items():
                if node_uuid != self.uuid and distance != float('inf'):
                    node_name = self.neighbor_names.get(node_uuid, node_uuid)
                    result[node_name] = distance
            return {'rank': result}

    def add_neighbor(self, uuid, host, backend_port, metric):
        with self.lock:
            self.peers[uuid] = {
                'host': host,
                'backend_port': backend_port,
                'metric': metric
            }

    def run(self):
        threading.Thread(target=self.receive_loop, daemon=True).start()
        threading.Thread(target=self.keepalive_loop, daemon=True).start()
        threading.Thread(target=self.lsa_loop, daemon=True).start()
        threading.Thread(target=self.timeout_loop, daemon=True).start()

        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:
                    break

                line = line.strip()
                if not line:
                    continue

                if line == 'uuid':
                    print(json.dumps(self.get_uuid()))
                    sys.stdout.flush()
                elif line == 'neighbors':
                    print(json.dumps(self.get_neighbors()))
                    sys.stdout.flush()
                elif line == 'map':
                    print(json.dumps(self.get_map()))
                    sys.stdout.flush()
                elif line == 'rank':
                    print(json.dumps(self.get_rank()))
                    sys.stdout.flush()
                elif line.startswith('addneighbor'):
                    parts = line.split()
                    params = {}
                    for part in parts[1:]:
                        key, val = part.split('=')
                        params[key] = val
                    self.add_neighbor(
                        params['uuid'],
                        params['host'],
                        int(params['backend_port']),
                        int(params['metric'])
                    )
                elif line == 'kill':
                    self.running = False
                    break
            except (EOFError, KeyboardInterrupt):
                break
            except Exception:
                pass

        self.sock.close()
        sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()

    server = ContentServer(args.config)
    server.run()
