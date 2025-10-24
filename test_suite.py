#!/usr/bin/env python3
"""
Test Suite for CMU 18-441/741 Project 2: Content Distribution
Tests link-state routing protocol implementation
"""

import subprocess
import time
import json
import os
import sys
import tempfile
from pathlib import Path

class TestRunner:
    def __init__(self):
        self.processes = []
        self.temp_dir = tempfile.mkdtemp()
        
    def create_config(self, name, uuid, port, peers):
        """Create a configuration file"""
        config_path = os.path.join(self.temp_dir, f"{name}.conf")
        with open(config_path, 'w') as f:
            f.write(f"uuid = {uuid}\n")
            f.write(f"name = {name}\n")
            f.write(f"backend_port = {port}\n")
            f.write(f"peer_count = {len(peers)}\n")
            for i, peer in enumerate(peers):
                f.write(f"peer_{i} = {peer['uuid']}, {peer['host']}, {peer['port']}, {peer['metric']}\n")
        return config_path
    
    def start_server(self, config_path):
        """Start a content server"""
        proc = subprocess.Popen(
            ['python3', 'content_server.py', '-c', config_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        self.processes.append(proc)
        return proc
    
    def send_command(self, proc, command):
        """Send command and get response"""
        try:
            proc.stdin.write(command + '\n')
            proc.stdin.flush()
            if command == 'kill':
                return None
            response = proc.stdout.readline().strip()
            return json.loads(response)
        except Exception as e:
            print(f"Error sending command '{command}': {e}")
            return None
    
    def cleanup(self):
        """Kill all processes and cleanup"""
        for proc in self.processes:
            try:
                proc.stdin.write('kill\n')
                proc.stdin.flush()
                proc.wait(timeout=2)
            except:
                proc.kill()
        
        # Cleanup temp files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

class TestSuite:
    def __init__(self):
        self.runner = TestRunner()
        self.passed = 0
        self.failed = 0
        
    def assert_equal(self, actual, expected, test_name):
        """Assert two values are equal"""
        if actual == expected:
            print(f"✓ {test_name}")
            self.passed += 1
            return True
        else:
            print(f"✗ {test_name}")
            print(f"  Expected: {expected}")
            print(f"  Actual:   {actual}")
            self.failed += 1
            return False
    
    def assert_in(self, item, collection, test_name):
        """Assert item is in collection"""
        if item in collection:
            print(f"✓ {test_name}")
            self.passed += 1
            return True
        else:
            print(f"✗ {test_name}")
            print(f"  Expected {item} in {collection}")
            self.failed += 1
            return False
    
    def test_1_simple_two_nodes(self):
        """Test 1: Two nodes with single link"""
        print("\n=== Test 1: Two Nodes ===")
        
        # Create configs
        node1_uuid = "11111111-1111-1111-1111-111111111111"
        node2_uuid = "22222222-2222-2222-2222-222222222222"
        
        config1 = self.runner.create_config(
            "node1", node1_uuid, 18001,
            [{"uuid": node2_uuid, "host": "localhost", "port": 18002, "metric": 10}]
        )
        
        config2 = self.runner.create_config(
            "node2", node2_uuid, 18002,
            [{"uuid": node1_uuid, "host": "localhost", "port": 18001, "metric": 10}]
        )
        
        # Start servers
        proc1 = self.runner.start_server(config1)
        proc2 = self.runner.start_server(config2)
        
        time.sleep(2)  # Wait for keepalives
        
        # Test uuid command
        uuid1 = self.runner.send_command(proc1, "uuid")
        self.assert_equal(uuid1['uuid'], node1_uuid, "Node1 UUID")
        
        uuid2 = self.runner.send_command(proc2, "uuid")
        self.assert_equal(uuid2['uuid'], node2_uuid, "Node2 UUID")
        
        # Wait for LSAs to propagate
        time.sleep(6)
        
        # Test neighbors
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        self.assert_in("node2", neighbors1.get('neighbors', {}), "Node1 sees Node2 as neighbor")
        if "node2" in neighbors1.get('neighbors', {}):
            self.assert_equal(neighbors1['neighbors']['node2']['metric'], 10, "Node1->Node2 metric")
        
        neighbors2 = self.runner.send_command(proc2, "neighbors")
        self.assert_in("node1", neighbors2.get('neighbors', {}), "Node2 sees Node1 as neighbor")
        
        # Test map
        map1 = self.runner.send_command(proc1, "map")
        self.assert_in("node1", map1.get('map', {}), "Node1 map contains node1")
        self.assert_in("node2", map1.get('map', {}), "Node1 map contains node2")
        
        # Test rank
        rank1 = self.runner.send_command(proc1, "rank")
        self.assert_equal(rank1.get('rank', {}).get('node2'), 10, "Node1 distance to Node2")
        
        rank2 = self.runner.send_command(proc2, "rank")
        self.assert_equal(rank2.get('rank', {}).get('node1'), 10, "Node2 distance to Node1")
    
    def test_2_triangle_topology(self):
        """Test 2: Three nodes in triangle"""
        print("\n=== Test 2: Triangle Topology ===")
        
        node1_uuid = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        node2_uuid = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        node3_uuid = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        
        config1 = self.runner.create_config(
            "node1", node1_uuid, 18011,
            [
                {"uuid": node2_uuid, "host": "localhost", "port": 18012, "metric": 10},
                {"uuid": node3_uuid, "host": "localhost", "port": 18013, "metric": 20}
            ]
        )
        
        config2 = self.runner.create_config(
            "node2", node2_uuid, 18012,
            [
                {"uuid": node1_uuid, "host": "localhost", "port": 18011, "metric": 10},
                {"uuid": node3_uuid, "host": "localhost", "port": 18013, "metric": 30}
            ]
        )
        
        config3 = self.runner.create_config(
            "node3", node3_uuid, 18013,
            [
                {"uuid": node1_uuid, "host": "localhost", "port": 18011, "metric": 20},
                {"uuid": node2_uuid, "host": "localhost", "port": 18012, "metric": 30}
            ]
        )
        
        proc1 = self.runner.start_server(config1)
        proc2 = self.runner.start_server(config2)
        proc3 = self.runner.start_server(config3)
        
        time.sleep(7)  # Wait for convergence
        
        # Test shortest paths (Dijkstra)
        rank1 = self.runner.send_command(proc1, "rank")
        self.assert_equal(rank1.get('rank', {}).get('node2'), 10, "Node1->Node2 shortest path")
        self.assert_equal(rank1.get('rank', {}).get('node3'), 20, "Node1->Node3 shortest path")
        
        rank2 = self.runner.send_command(proc2, "rank")
        self.assert_equal(rank2.get('rank', {}).get('node1'), 10, "Node2->Node1 shortest path")
        self.assert_equal(rank2.get('rank', {}).get('node3'), 30, "Node2->Node3 shortest path")
        
        rank3 = self.runner.send_command(proc3, "rank")
        self.assert_equal(rank3.get('rank', {}).get('node1'), 20, "Node3->Node1 shortest path")
        self.assert_equal(rank3.get('rank', {}).get('node2'), 30, "Node3->Node2 shortest path")
        
        # Test map
        map1 = self.runner.send_command(proc1, "map")
        self.assert_equal(len(map1.get('map', {})), 3, "Node1 sees 3 nodes in map")
    
    def test_3_node_failure(self):
        """Test 3: Node failure detection"""
        print("\n=== Test 3: Node Failure Detection ===")
        
        node1_uuid = "d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1"
        node2_uuid = "d2d2d2d2-d2d2-d2d2-d2d2-d2d2d2d2d2d2"
        
        config1 = self.runner.create_config(
            "node1", node1_uuid, 18021,
            [{"uuid": node2_uuid, "host": "localhost", "port": 18022, "metric": 5}]
        )
        
        config2 = self.runner.create_config(
            "node2", node2_uuid, 18022,
            [{"uuid": node1_uuid, "host": "localhost", "port": 18021, "metric": 5}]
        )
        
        proc1 = self.runner.start_server(config1)
        proc2 = self.runner.start_server(config2)
        
        time.sleep(5)
        
        # Verify connection
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        self.assert_in("node2", neighbors1.get('neighbors', {}), "Node1 initially sees Node2")
        
        # Kill node2
        self.runner.send_command(proc2, "kill")
        proc2.wait(timeout=2)
        
        # Wait for timeout
        time.sleep(12)
        
        # Verify node2 removed from neighbors
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        self.assert_equal(len(neighbors1.get('neighbors', {})), 0, "Node1 detects Node2 failure")
    
    def test_4_add_neighbor(self):
        """Test 4: Dynamic neighbor addition"""
        print("\n=== Test 4: Add Neighbor ===")
        
        node1_uuid = "e1e1e1e1-e1e1-e1e1-e1e1-e1e1e1e1e1e1"
        node2_uuid = "e2e2e2e2-e2e2-e2e2-e2e2-e2e2e2e2e2e2"
        
        # Start with no peers
        config1 = self.runner.create_config("node1", node1_uuid, 18031, [])
        config2 = self.runner.create_config("node2", node2_uuid, 18032, [])
        
        proc1 = self.runner.start_server(config1)
        proc2 = self.runner.start_server(config2)
        
        time.sleep(2)
        
        # Verify no neighbors initially
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        self.assert_equal(len(neighbors1.get('neighbors', {})), 0, "Node1 initially has no neighbors")
        
        # Add neighbor from node1
        self.runner.send_command(proc1, f"addneighbor uuid={node2_uuid} host=localhost backend_port=18032 metric=15")
        
        time.sleep(8)  # Wait for keepalives and LSAs
        
        # Verify bidirectional connection
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        self.assert_in("node2", neighbors1.get('neighbors', {}), "Node1 sees Node2 after addneighbor")
        
        neighbors2 = self.runner.send_command(proc2, "neighbors")
        self.assert_in("node1", neighbors2.get('neighbors', {}), "Node2 auto-discovers Node1")
    
    def test_5_complex_topology(self):
        """Test 5: Complex 4-node topology with shortest path"""
        print("\n=== Test 5: Complex Topology (4 nodes) ===")
        
        # Topology:
        #   A --10-- B
        #   |        |
        #   20      20
        #   |        |
        #   C --30-- D
        
        uuids = {
            'A': "aaaaaaaa-1111-1111-1111-111111111111",
            'B': "bbbbbbbb-2222-2222-2222-222222222222",
            'C': "cccccccc-3333-3333-3333-333333333333",
            'D': "dddddddd-4444-4444-4444-444444444444"
        }
        
        configs = {
            'A': self.runner.create_config("nodeA", uuids['A'], 18041, [
                {"uuid": uuids['B'], "host": "localhost", "port": 18042, "metric": 10},
                {"uuid": uuids['C'], "host": "localhost", "port": 18043, "metric": 20}
            ]),
            'B': self.runner.create_config("nodeB", uuids['B'], 18042, [
                {"uuid": uuids['A'], "host": "localhost", "port": 18041, "metric": 10},
                {"uuid": uuids['D'], "host": "localhost", "port": 18044, "metric": 20}
            ]),
            'C': self.runner.create_config("nodeC", uuids['C'], 18043, [
                {"uuid": uuids['A'], "host": "localhost", "port": 18041, "metric": 20},
                {"uuid": uuids['D'], "host": "localhost", "port": 18044, "metric": 30}
            ]),
            'D': self.runner.create_config("nodeD", uuids['D'], 18044, [
                {"uuid": uuids['B'], "host": "localhost", "port": 18042, "metric": 20},
                {"uuid": uuids['C'], "host": "localhost", "port": 18043, "metric": 30}
            ])
        }
        
        procs = {
            'A': self.runner.start_server(configs['A']),
            'B': self.runner.start_server(configs['B']),
            'C': self.runner.start_server(configs['C']),
            'D': self.runner.start_server(configs['D'])
        }
        
        time.sleep(8)
        
        # Test shortest paths from A
        rankA = self.runner.send_command(procs['A'], "rank")
        self.assert_equal(rankA.get('rank', {}).get('nodeB'), 10, "A->B: 10")
        self.assert_equal(rankA.get('rank', {}).get('nodeC'), 20, "A->C: 20")
        self.assert_equal(rankA.get('rank', {}).get('nodeD'), 30, "A->D: 30 (via B, not C)")
        
        # Test map completeness
        mapA = self.runner.send_command(procs['A'], "map")
        self.assert_equal(len(mapA.get('map', {})), 4, "All 4 nodes in map")
    
    def test_6_output_format(self):
        """Test 6: Verify output format is evaluable"""
        print("\n=== Test 6: Output Format Validation ===")
        
        node_uuid = "f1f1f1f1-f1f1-f1f1-f1f1-f1f1f1f1f1f1"
        config = self.runner.create_config("node1", node_uuid, 18051, [])
        proc = self.runner.start_server(config)
        
        time.sleep(2)
        
        # Test that outputs are valid JSON and evaluable
        commands = ['uuid', 'neighbors', 'map', 'rank']
        for cmd in commands:
            response = self.runner.send_command(proc, cmd)
            is_dict = isinstance(response, dict)
            self.assert_equal(is_dict, True, f"{cmd} returns evaluable dict")
    
    def test_7_localhost_normalization(self):
        """Test 7: Host address normalization"""
        print("\n=== Test 7: Localhost Normalization ===")
        
        node1_uuid = "g1g1g1g1-g1g1-g1g1-g1g1-g1g1g1g1g1g1"
        node2_uuid = "g2g2g2g2-g2g2-g2g2-g2g2-g2g2g2g2g2g2"
        
        # Use 127.0.0.1 in config
        config1 = self.runner.create_config(
            "node1", node1_uuid, 18061,
            [{"uuid": node2_uuid, "host": "127.0.0.1", "port": 18062, "metric": 5}]
        )
        
        config2 = self.runner.create_config(
            "node2", node2_uuid, 18062,
            [{"uuid": node1_uuid, "host": "localhost", "port": 18061, "metric": 5}]
        )
        
        proc1 = self.runner.start_server(config1)
        proc2 = self.runner.start_server(config2)
        
        time.sleep(5)
        
        neighbors1 = self.runner.send_command(proc1, "neighbors")
        if 'node2' in neighbors1.get('neighbors', {}):
            host = neighbors1['neighbors']['node2'].get('host', '')
            self.assert_equal(host, 'localhost', "Host normalized to 'localhost'")
    
    def run_all_tests(self):
        """Run all tests"""
        print("=" * 60)
        print("CMU 18-441/741 Project 2 Test Suite")
        print("=" * 60)
        
        tests = [
            self.test_1_simple_two_nodes,
            self.test_2_triangle_topology,
            self.test_3_node_failure,
            self.test_4_add_neighbor,
            self.test_5_complex_topology,
            self.test_6_output_format,
            self.test_7_localhost_normalization
        ]
        
        for test in tests:
            try:
                test()
            except Exception as e:
                print(f"✗ Test failed with exception: {e}")
                import traceback
                traceback.print_exc()
                self.failed += 1
            finally:
                self.runner.cleanup()
                self.runner = TestRunner()  # Fresh runner for next test
                time.sleep(1)  # Cool down between tests
        
        print("\n" + "=" * 60)
        print(f"Results: {self.passed} passed, {self.failed} failed")
        print("=" * 60)
        
        return self.failed == 0

if __name__ == '__main__':
    # Check if content_server.py exists
    if not os.path.exists('content_server.py'):
        print("Error: content_server.py not found in current directory")
        sys.exit(1)
    
    suite = TestSuite()
    success = suite.run_all_tests()
    sys.exit(0 if success else 1)
