# Durable benchmark

`run.sh` builds the Node and benchmark binaries, starts three independent local
processes with separate data directories, runs the public client API, and
removes the processes and data on exit. On Windows it uses `go run` because
local executable application-control policies may reject newly built binaries;
the process tree is still terminated during cleanup.

```sh
sh benchmark/run.sh benchmark/results/my-machine.json \
  -hardware "CPU, cores, RAM, disk" \
  -set-operations 500 -get-operations 2000 \
  -concurrency 8 -value-bytes 1024
```

The exact durable configuration is used: every mutation goes through the
normal Raft path and the WAL's `File.Sync`; there is no unsafe-acknowledgement
or benchmark-only storage mode. The benchmark uses the public `client.Client`
API. Each concurrent worker owns one Client Session and serializes its SET
sequences. GETs are linearizable API reads. The JSON report contains the
command parameters, environment, workload duration, throughput, p50/p95/p99
latencies, and latency samples in acquisition order. It is the raw result
format and can be reprocessed without rerunning the workload.

## Published measurement

`results/windows-ryzen7-8845hs.json` was produced from this checkout on
Windows 11 Home Single Language (10.0.26200), AMD Ryzen 7 8845HS, 8 cores / 16
threads, 15.3 GiB RAM, NVMe SSD, Go 1.26.5, with three local processes,
1 KiB Values, eight concurrent workers, 500 SETs, and 2,000 GETs:

| Command | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| SET | 715.1 ops/s | 11.03 ms | 14.37 ms | 15.50 ms |
| GET | 1,028.1 ops/s | 7.63 ms | 10.14 ms | 11.52 ms |

These are local development measurements, not production claims. Results are
sensitive to filesystem, process placement, payload, and concurrency; compare
only reports with their metadata.
