#!/usr/bin/env bash
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work=$(mktemp -d)
cleanup() {
  kill "${worker_a:-}" "${worker_b:-}" "${controller:-}" 2>/dev/null || true
  wait "${worker_a:-}" "${worker_b:-}" "${controller:-}" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

cd "$root"
go build -o "$work/controller" ./cmd/controller
go build -o "$work/worker" ./cmd/worker
go build -o "$work/orbit" ./cmd/orbit

"$work/controller" -addr 127.0.0.1:19000 -metrics-addr 127.0.0.1:19090 & controller=$!
"$work/worker" -controller 127.0.0.1:19000 -id worker-b -duration 10s & worker_b=$!
"$work/worker" -controller 127.0.0.1:19000 -id worker-a -duration 10s & worker_a=$!
sleep 2
"$work/orbit" submit -controller 127.0.0.1:19000 -id demo-1 -cpu 1 -memory-mb 512
kill "$worker_a"
sleep 1
"$work/orbit" status -controller 127.0.0.1:19000 -id demo-1
sleep 11
"$work/orbit" status -controller 127.0.0.1:19000 -id demo-1
