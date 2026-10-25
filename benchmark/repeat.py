#!/usr/bin/env python3
"""Run repeated checked public-API workloads against fresh local clusters."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics
import socket
import subprocess
import tempfile
import time
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--runs', type=int, default=5)
parser.add_argument('--set-operations', type=int, default=1000)
parser.add_argument('--get-operations', type=int, default=1500)
parser.add_argument('--profile', action='store_true')
args = parser.parse_args()
assert args.runs >= 5
root = Path(__file__).resolve().parents[1]
args.output.mkdir(parents=True, exist_ok=False)
binaries = {name: root / '.benchmark-work' / name for name in ['ternion', 'ternionbench']}
report = {'host': platform.platform(), 'hardware': subprocess.check_output(['sysctl', '-n', 'machdep.cpu.brand_string'], text=True).strip(), 'memory_bytes': int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'], text=True)), 'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), 'benchmark_diff': subprocess.check_output(['git', 'diff', '--', 'cmd/ternionbench/main.go'], cwd=root, text=True), 'binary_sha256': {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()}, 'method': 'Fresh three-process cluster and temporary data directories per trial. 20 SET and 20 GET warmup operations excluded. Timed GETs validate returned bytes. Normal File.Sync path. Elected leader ordered first in client addresses after warmup. Eight workers, 1024-byte values. Profiling trial excluded from summary.', 'trials': []}
for trial in range(args.runs + int(args.profile)):
    profiling = trial == args.runs
    trial_dir = args.output / ('profile' if profiling else f'trial-{trial + 1}')
    trial_dir.mkdir()
    nodes = []
    logs = []
    with tempfile.TemporaryDirectory(prefix='ternion-resume-') as work:
        try:
            for port in [17301, 17302, 17303, 17401, 17402, 17403, 17501, 17502, 17503]:
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(('127.0.0.1', port))
                    probe.listen(1)
            for node in range(1, 4):
                config = Path(work) / f'node-{node}.yaml'
                config.write_text(f'version: 1\ncluster_id: resume-benchmark\nactive_session_limit: 1024\nsnapshot_threshold_bytes: 67108864\nnode:\n  id: node-{node}\n  data_dir: {work}/data/node-{node}\n  metrics_address: 127.0.0.1:{17500 + node}\nmembers:\n' + ''.join(f'  node-{peer}:\n    peer_address: 127.0.0.1:{17300 + peer}\n    client_address: 127.0.0.1:{17400 + peer}\n' for peer in range(1, 4)))
                log = (trial_dir / f'node-{node}.log').open('w')
                logs.append(log)
                nodes.append(subprocess.Popen([str(binaries['ternion']), '-config', str(config)], stdout=log, stderr=log))
            command = [str(binaries['ternionbench']), '-addresses', '127.0.0.1:17401,127.0.0.1:17402,127.0.0.1:17403', '-hardware', report['hardware'] + ', 10 cores, 16 GiB RAM, local APFS temporary directories', '-concurrency', '8', '-value-bytes', '1024']
            subprocess.run(command + ['-set-operations', '20', '-get-operations', '20', '-output', str(trial_dir / 'warmup.json')], check=True, timeout=180)
            for node in range(1, 4):
                (trial_dir / f'metrics-before-{node}.txt').write_bytes(urllib.request.urlopen(f'http://127.0.0.1:{17500 + node}/metrics', timeout=5).read())
            assert all(process.poll() is None for process in nodes), 'A benchmark node exited'
            proposal_counts = []
            for node in range(1, 4):
                metrics = dict(line.split() for line in (trial_dir / f'metrics-before-{node}.txt').read_text().splitlines())
                proposal_counts.append(int(metrics['ternion_proposals_total']))
            leader = max(range(3), key=lambda node: proposal_counts[node]) + 1
            assert proposal_counts[leader - 1] > 0
            node_order = [leader] + [node for node in range(1, 4) if node != leader]
            command[2] = ','.join(f'127.0.0.1:{17400 + node}' for node in node_order)
            load = subprocess.check_output(['uptime'], text=True).strip()
            child = subprocess.Popen(command + ['-set-operations', str(500 if profiling else args.set_operations), '-get-operations', str(1000 if profiling else args.get_operations), '-output', str(trial_dir / 'raw.json')], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            samplers = []
            if profiling:
                samplers = [subprocess.Popen(['sample', str(pid), '3', '-file', str(trial_dir / f'{name}-sample.txt')], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for name, pid in [('client', child.pid), ('node-1', nodes[0].pid)]]
            counters = []
            while child.poll() is None:
                counters.append({'monotonic': time.monotonic(), 'processes': subprocess.check_output(['ps', '-p', ','.join(str(p.pid) for p in nodes + [child]), '-o', 'pid=,pcpu=,rss=,time=,comm='], text=True)})
                time.sleep(1)
            stdout, stderr = child.communicate()
            (trial_dir / 'client.stderr.txt').write_text(stderr)
            (trial_dir / 'process-counters.json').write_text(json.dumps(counters, indent=2) + '\n')
            assert child.returncode == 0, stderr
            for sampler in samplers:
                sampler.wait(timeout=30)
            for node in range(1, 4):
                (trial_dir / f'metrics-after-{node}.txt').write_bytes(urllib.request.urlopen(f'http://127.0.0.1:{17500 + node}/metrics', timeout=5).read())
            raw = json.loads((trial_dir / 'raw.json').read_text())
            for workload in raw['workloads']:
                assert workload['operations'] == len(workload['latency_samples_nanoseconds'])
                assert all(value > 0 for value in workload['latency_samples_nanoseconds'])
            report['trials'].append({'trial': trial + 1, 'profiling': profiling, 'leader_first': leader, 'load': load, 'process_exit_code': child.returncode, 'raw': raw})
            (args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
            print(('profile' if profiling else f'trial {trial + 1}') + ': ' + ', '.join(f"{w['command']} {w['throughput_ops_per_second']:.1f} ops/s" for w in raw['workloads']), flush=True)
        finally:
            for process in nodes:
                process.terminate()
            for process in nodes:
                process.wait(timeout=20)
            for log in logs:
                log.close()
report['summary'] = {}
for command in ['SET', 'GET']:
    rows = [w for trial in report['trials'] if not trial['profiling'] for w in trial['raw']['workloads'] if w['command'] == command]
    report['summary'][command] = {key: {'median': statistics.median(row[key] for row in rows), 'range': [min(row[key] for row in rows), max(row[key] for row in rows)]} for key in ['throughput_ops_per_second', 'p50_nanoseconds', 'p95_nanoseconds', 'p99_nanoseconds', 'duration_nanoseconds']}
report['successful_measured_runs'] = args.runs
report['failed_measured_runs'] = 0
(args.output / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report['summary'], indent=2))
