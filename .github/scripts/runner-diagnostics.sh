#!/usr/bin/env bash
# Compare before/after counters to distinguish slow tests from runner contention.
set -euo pipefail
export LC_ALL=C

output=${1:?Usage: runner-diagnostics.sh OUTPUT_DIRECTORY}
mkdir -p "$output"

capture() {
    local name=$1
    shift
    # Diagnostics are best effort, including on a partially broken Docker host.
    timeout 15 "$@" > "$output/$name.txt" 2>&1 || true
}

# Deliberately allowlist metadata instead of exporting the job's environment.
{
    for name in RUNNER_NAME RUNNER_ENVIRONMENT RUNNER_OS RUNNER_ARCH \
        ImageOS ImageVersion GITHUB_RUN_ID GITHUB_RUN_ATTEMPT GITHUB_SHA \
        CLICKHOUSE_VERSION INTEGRATION_JOBS; do
        printf '%s=%s\n' "$name" "${!name-<unset>}"
    done
    printf '%s\n' 'Actual slot count and worker images: integration-test-results summary.json.'
    printf '%s\n' 'Host counters may include other jobs on shared runners.'
} > "$output/runner.txt"

capture timestamp date --utc --iso-8601=ns
capture kernel uname -srvmo
capture cpu lscpu
capture affinity awk '/^(Cpus_allowed_list|Mems_allowed_list):/' /proc/self/status
capture memory free -m
capture disk-space df -hT .
capture inodes df -i .
capture filesystem findmnt -T . -o TARGET,SOURCE,FSTYPE
for name in stat meminfo loadavg uptime diskstats vmstat pressure/cpu pressure/io pressure/memory; do
    capture "proc-${name//\//-}" cat "/proc/$name"
done

# A limit may belong to a parent cgroup. Record the current cgroup and ancestors,
# without walking sibling groups or inspecting other jobs' processes/containers.
capture cgroup-membership cat /proc/self/cgroup
capture cgroup-mounts findmnt -t cgroup,cgroup2 -o TARGET,FSROOT,FSTYPE
{
    if [ -f /sys/fs/cgroup/cgroup.controllers ]; then
        group=$(awk -F: '$1 == "0" { print $3 }' /proc/self/cgroup)
        directory=$(realpath -e -- "/sys/fs/cgroup${group}" 2>/dev/null || true)
        case "$directory" in
            /sys/fs/cgroup|/sys/fs/cgroup/*) ;;
            *)
                printf '%s\n' 'Current cgroup path is not visible; recording the mounted root.'
                directory=/sys/fs/cgroup
                ;;
        esac
        while :; do
            printf '\n=== %s ===\n' "$directory"
            for name in cpu.max cpu.weight cpu.stat cpuset.cpus.effective \
                memory.max memory.current memory.events io.stat \
                cpu.pressure memory.pressure io.pressure; do
                if [ -r "$directory/$name" ]; then
                    printf '\n--- %s ---\n' "$name"
                    cat "$directory/$name" || true
                fi
            done
            [ "$directory" != /sys/fs/cgroup ] || break
            directory=${directory%/*}
        done
    else
        printf '%s\n' 'Cgroup v2 counters unavailable; see cgroup membership and mounts.'
    fi
} > "$output/cgroup-counters.txt" 2>&1

capture python .venv/bin/python -VV
capture uv uv --version
capture python-packages uv --offline pip list --python .venv/bin/python --format=json
capture docker-version docker version
capture docker-compose-version docker compose version
capture docker-info docker info --format 'Server={{.ServerVersion}} Kernel={{.KernelVersion}} Architecture={{.Architecture}} Storage={{.Driver}} Cgroup={{.CgroupDriver}} CgroupVersion={{.CgroupVersion}} CPUs={{.NCPU}} Memory={{.MemTotal}} Containers={{.Containers}} Running={{.ContainersRunning}}'
