# 04 — Resources & Tuning

Sized for a 32 GB / 24-core (i9 14th gen) laptop. The stack can never take more
than ~16 GiB, leaving room for the OS, IDE and browser.

## Per-container budget (`deploy.resources` in docker-compose.yml)

| Service | CPU limit | Mem limit | Mem reservation | In-process sizing | Idle usage seen |
|---------|-----------|-----------|-----------------|-------------------|-----------------|
| spark | 6.0 | 6 GiB | 3 GiB | `local[4]`, driver 3 GB (`spark-defaults.conf`) | ~0.5 GiB (peaks during jobs) |
| trino | 4.0 | 4 GiB | 2 GiB | JVM `MaxRAMPercentage=80` → ~3.2 GiB heap | ~1.0 GiB |
| kafka-connect | 2.0 | 2 GiB | 1 GiB | `KAFKA_HEAP_OPTS=-Xms512m -Xmx1g` | ~0.8 GiB |
| redpanda | 2.0 | 2 GiB | 1 GiB | `--smp 1 --memory 1G` (Seastar) | ~0.3 GiB |
| minio | 1.0 | 1 GiB | 256 MiB | — | ~0.4 GiB |
| redis | 1.0 | 512 MiB | 128 MiB | `maxmemory 384mb` | <10 MiB |
| producer | 1.0 | 256 MiB | — | on demand only | — |
| *-init (×3) | ≤0.5 | ≤128 MiB | — | exit after a few seconds | — |
| **Total (long-running)** | **16** | **~15.5 GiB** | **~7.4 GiB** | | **~3 GiB idle** |

Rules applied:

- **JVM services get a container limit ~25–35% above their heap.** Off-heap
  memory (metaspace, direct buffers, threads, Python workers for Spark) needs
  room, or the kernel OOM-killer hits before the JVM can throw a clean OOM.
- **Seastar (Redpanda) is told its memory explicitly.** Otherwise it tries to
  grab most of the host's RAM.
- **Redis `maxmemory` < container limit**, so Redis evicts keys before the kernel kills it.

### How Docker applies these (non-Swarm Compose)

| Setting | Effect with plain Docker Engine |
|---------|--------------------------------|
| `limits.memory` | Hard cgroup limit (container is OOM-killed above it) |
| `limits.cpus` | Hard CFS quota |
| `reservations.memory` | Soft limit (`--memory-reservation`): reclaimed first only under host memory pressure |
| `reservations.cpus` | **Not enforced** outside Swarm. It only documents the expected baseline. |

Check: `docker stats` or `docker inspect spark --format '{{.HostConfig.Memory}} {{.HostConfig.MemoryReservation}}'`.

## Engine-level settings: do you need to change anything?

Find out which engine you're using with `docker context show`.

### Native Docker Engine on Linux (`default` context): no changes required

Containers run directly on the host kernel and see all 32 GB / 32 threads. The
compose limits are the only caps. Optional host checks:

```bash
free -g                                  # keep >= 16 GiB available before `up`
cat /proc/sys/fs/aio-max-nr              # Redpanda: >= 1048576 recommended
sudo sysctl -w fs.aio-max-nr=1048576     # only if lower (persist in /etc/sysctl.d/ if you want)
```

### Docker Desktop (`desktop-linux` context, or Windows/macOS)

Desktop runs a VM with its **own** RAM/CPU budget, and everything (including
image builds) has to fit inside it. Before `docker compose up`:

*Settings → Resources → Advanced*

| Setting | Minimum | Recommended (32 GB host) |
|---------|---------|--------------------------|
| CPUs | 8 | **12** |
| Memory | 16 GB | **18–20 GB** |
| Swap | 1 GB | 2 GB |
| Virtual disk limit | 40 GB | 64 GB |

Then *Apply & restart*. On Windows with the WSL2 backend, the budget is set in
`%UserProfile%\.wslconfig` instead (`[wsl2]` → `memory=20GB`, `processors=12`),
followed by `wsl --shutdown`.

Both engines are installed on this machine. Run the lab against **one** of them,
and remember that images and volumes aren't shared between contexts.

## Scaling knobs (when you want to push more volume)

| Want | Change |
|------|--------|
| Faster ETL | Raise `spark` limits and `spark.master local[N]`, `spark.driver.memory` |
| Bigger Trino queries | Raise the `trino` mem limit (the heap follows at 80%) |
| More ingest parallelism | More topic partitions **and** `tasks.max` in the connector |
| Bigger bronze files | Raise `flush.size` / `rotate.*.interval.ms` (see [05](05-bronze-kafka-connect.md)) |
