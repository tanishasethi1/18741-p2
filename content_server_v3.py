#!/usr/bin/env python3

import socket
import sys
import threading
import time
import json
import argparse
from collections import defaultdict

# Constants
KEEPALIVE_INTERVAL = 3  # Send keepalive every 3 seconds
KEEPALIVE_TIMEOUT = 9   # Miss 3 consecutive keepalives (3 * 3 = 9 seconds)
LSA_INTERVAL = 5        # Send LSA every 5 seconds
BUFSIZE = 4096

class ContentServer:
    def __init__(self, config_file):
        self.config = self.parse_config(config_file)
        self.uuid = self.config['uuid']
        self.name = self.config['name']
        self.backend_port = self.config['backend_port']
        self.peers = self.config['peers']  # Dict: uuid -> {host, port, metric}
        
        # Neighbor management
        self.neighbors = {}  # uuid -> {name, host, port, metric, last_seen}
        self.neighbor_names = {}  # uuid -> name (learned from LSAs)
        
        # Link state database
        self.lsdb = {}  # uuid -> {seq_num, neighbors: {uuid: metric}}
        self.lsdb[self.uuid] = {'seq_num': 0, 'neighbors': {}}
        
        # Sequence number for our LSAs
        self.seq_num = 0
        
        # Socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', self.backend_port))
        
        # Synchronization
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
                    key = key.strip()
                    value = value.strip()
                    
                    if key == 'uuid':
                        config['uuid'] = value
                    elif key == 'name':
                        config['name'] = value
                    elif key == 'backend_port':
                        config['backend_port'] = int(value)
                    elif key == 'peer_count':
                        config['peer_count'] = int(value)
                    elif key.startswith('peer_'):
                        parts = [p.strip() for p in value.split(',')]
                        peer_uuid = parts[0]
                        peer_host = parts[1]
                        peer_port = int(parts[2])
                        peer_metric = int(parts[3])
                        peers[peer_uuid] = {
                            'host': peer_host,
                            'backend_port': peer_port,
                            'metric': peer_metric
                        }
        
        config['peers'] = peers
        return config
    
    def send_message(self, msg, host, port):
        try:
            # Handle localhost variants
            if host in ['localhost', '127.0.0.1', '']:
                host = 'localhost'
            self.sock.sendto(json.dumps(msg).encode(), (host, port))
        except Exception as e:
            pass
    
    def send_keepalive(self):
        """Send keepalive messages to all configured peers"""
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
        """Send link state advertisement"""
        with self.lock:
            # Build our neighbors list for LSA
            neighbors = {}
            for peer_uuid, peer_info in self.peers.items():
                if peer_uuid in self.neighbors:
                    neighbors[peer_uuid] = peer_info['metric']
            
            # Update our LSDB entry
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
            
            # Flood to all active neighbors
            for peer_uuid, peer_info in self.peers.items():
                if peer_uuid in self.neighbors:
                    self.send_message(msg, peer_info['host'], peer_info['backend_port'])
    
    def process_keepalive(self, msg, addr):
        """Process received keepalive message"""
        peer_uuid = msg['uuid']
        peer_name = msg['name']
        
        with self.lock:
            # Update neighbor status
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
                # This is a new neighbor (from addneighbor on other side)
                # Extract host from addr
                host, port = addr
                self.peers[peer_uuid] = {
                    'host': host,
                    'backend_port': port,
                    'metric': 1  # Default metric
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
        """Process received LSA message"""
        peer_uuid = msg['uuid']
        peer_name = msg['name']
        peer_seq = msg['seq_num']
        peer_neighbors = msg['neighbors']
        
        with self.lock:
            # Store name mapping
            self.neighbor_names[peer_uuid] = peer_name
            
            # Check if we should update our LSDB
            should_flood = False
            
            if peer_uuid not in self.lsdb:
                should_flood = True
            elif peer_seq > self.lsdb[peer_uuid]['seq_num']:
                should_flood = True
            
            if should_flood:
                # Update LSDB
                self.lsdb[peer_uuid] = {
                    'seq_num': peer_seq,
                    'neighbors': peer_neighbors
                }
                
                # Forward to all neighbors except the one we received from
                sender_host, sender_port = addr
                for peer_uuid_fwd, peer_info in self.peers.items():
                    if peer_uuid_fwd in self.neighbors:
                        # Don't send back to sender
                        if not (peer_info['host'] == sender_host and 
                               peer_info['backend_port'] == sender_port):
                            self.send_message(msg, peer_info['host'], 
                                            peer_info['backend_port'])
    
    def check_neighbor_timeouts(self):
        """Remove neighbors that haven't sent keepalive in timeout period"""
        current_time = time.time()
        with self.lock:
            to_remove = []
            for peer_uuid, neighbor_info in self.neighbors.items():
                if current_time - neighbor_info['last_seen'] > KEEPALIVE_TIMEOUT:
                    to_remove.append(peer_uuid)
            
            for peer_uuid in to_remove:
                del self.neighbors[peer_uuid]
    
    def receive_loop(self):
        """Main receive loop for UDP messages"""
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
            except Exception as e:
                pass
    
    def keepalive_loop(self):
        """Periodic keepalive sender"""
        while self.running:
            self.send_keepalive()
            time.sleep(KEEPALIVE_INTERVAL)
    
    def lsa_loop(self):
        """Periodic LSA sender"""
        time.sleep(1)  # Initial delay
        while self.running:
            self.send_lsa()
            time.sleep(LSA_INTERVAL)
    
    def timeout_loop(self):
        """Periodic neighbor timeout checker"""
        while self.running:
            self.check_neighbor_timeouts()
            time.sleep(1)
    
    def dijkstra(self):
        """Run Dijkstra's algorithm to find shortest paths"""
        # Build graph from LSDB
        graph = {}
        for node_uuid, lsa in self.lsdb.items():
            graph[node_uuid] = lsa['neighbors']
        
        # Dijkstra's algorithm
        distances = {self.uuid: 0}
        visited = set()
        
        while len(visited) < len(graph):
            # Find unvisited node with minimum distance
            current = None
            min_dist = float('inf')
            for node in graph:
                if node not in visited and distances.get(node, float('inf')) < min_dist:
                    min_dist = distances.get(node, float('inf'))
                    current = node
            
            if current is None:
                break
            
            visited.add(current)
            
            # Update distances to neighbors
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
            for peer_uuid, neighbor_info in self.neighbors.items():
                name = neighbor_info['name']
                # Normalize host to 'localhost' for consistency
                host = neighbor_info['host']
                if host in ['127.0.0.1', '']:
                    host = 'localhost'
                result[name] = {
                    'uuid': peer_uuid,
                    'host': host,
                    'backend_port': neighbor_info['backend_port'],
                    'metric': neighbor_info['metric']
                }
            return {'neighbors': result}
    
    def get_map(self):
        with self.lock:
            result = {}
            # Only include nodes that are in LSDB (active or reachable)
            for node_uuid, lsa in self.lsdb.items():
                node_name = self.neighbor_names.get(node_uuid, 
                           self.name if node_uuid == self.uuid else node_uuid)
                neighbors = {}
                # Only include neighbors that exist in the LSDB
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
        # Start background threads
        threading.Thread(target=self.receive_loop, daemon=True).start()
        threading.Thread(target=self.keepalive_loop, daemon=True).start()
        threading.Thread(target=self.lsa_loop, daemon=True).start()
        threading.Thread(target=self.timeout_loop, daemon=True).start()
        
        # Command processing loop
        while self.running:
            try:
                line = sys.stdin.readline()
                if not line:  # EOF
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
                    self.add_neighbor(params['uuid'], params['host'], 
                                    int(params['backend_port']), 
                                    int(params['metric']))
                    # No output for addneighbor
                elif line == 'kill':
                    self.running = False
                    break
                    
            except EOFError:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                # Silently handle errors as per requirements
                pass
        
        self.sock.close()
        sys.exit(0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True)
    args = parser.parse_args()
    
    server = ContentServer(args.config)
    server.run()
