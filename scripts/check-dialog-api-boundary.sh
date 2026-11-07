#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Ordinary builds retain BOTH APIs, even without default Cargo features.
cargo check "$@" --locked --no-default-features --features platform-tokio --lib

# Internal architectural audit, not a consumer-facing Cargo feature. This cfg
# deliberately omits legacy adapters and hides compatibility module paths.
RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg rsipstack_no_legacy" \
  cargo test "$@" --locked --no-default-features --features platform-tokio \
  --test dialog_operations --test managed_dialogs --test managed_publication --test managed_subscription
