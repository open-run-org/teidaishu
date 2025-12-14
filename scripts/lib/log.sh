#!/usr/bin/env bash
set -euo pipefail

log_info() { printf "[INFO] %s\n" "$*" >&2; }
log_warn() { printf "[WARN] %s\n" "$*" >&2; }
log_error() { printf "[ERROR] %s\n" "$*" >&2; }

task_start() {
  TASK_NAME="$1"
  TASK_T0_EPOCH="$(date -u +%s)"
  TASK_T0_ISO="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  printf "[TIME] %s start=%s\n" "$TASK_NAME" "$TASK_T0_ISO" >&2
}

task_end() {
  local name="$1"
  local t1_epoch
  local t1_iso
  local dt
  t1_epoch="$(date -u +%s)"
  t1_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  dt=$((t1_epoch - TASK_T0_EPOCH))
  printf "[TIME] %s end=%s elapsed=%ss\n" "$name" "$t1_iso" "$dt" >&2
}
