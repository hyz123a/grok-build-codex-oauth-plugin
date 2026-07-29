#!/usr/bin/env sh
set -eu

if [ -x "${GROK_PLUGIN_ROOT}/bin/grok-codex-oauth" ]; then
  exec "${GROK_PLUGIN_ROOT}/bin/grok-codex-oauth" "$@"
fi

exec grok-codex-oauth "$@"
